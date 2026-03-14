import copy
import json
import sys
from pathlib import Path

from dotenv import dotenv_values, set_key


def _base_dir() -> Path:
    """Project root: next to the .exe when frozen, else the repo root."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parents[3]


BASE_DIR = _base_dir()
ENV_PATH = BASE_DIR / ".env"
CONFIG_DIR = BASE_DIR / "config"
STATE_DIR = CONFIG_DIR / "state"
RUN_CONFIG_PATH = STATE_DIR / "run_config.json"
SAVED_SETUPS_DIR = STATE_DIR / "saved_setups"
APP_STATE_PATH = STATE_DIR / "app_state.json"
ASSETS_DIR = BASE_DIR / "assets"


STEP_CONFIG_KEY: dict[str, str] = {
    "get_documents": "get_documents_config",
    "download_documents": "download_documents_config",
    "populate_company_info": "populate_company_info_config",
    "import_stock_prices_csv": "import_stock_prices_csv_config",
    "update_stock_prices": "update_stock_prices_config",
    "parse_taxonomy": "parse_taxonomy_config",
    "generate_financial_statements": "generate_financial_statements_config",
    "generate_ratios": "generate_ratios_config",
    "generate_historical_ratios": "generate_historical_ratios_config",
    "find_significant_predictors": "find_significant_predictors_config",
    "Multivariate_Regression": "Multivariate_Regression_config",
    "backtest": "backtesting_config",
    "backtest_set": "backtest_set_config",
}

STEP_DISPLAY: dict[str, str] = {
    "get_documents": "Get Documents",
    "download_documents": "Download Documents",
    "standardize_data": "Standardize Data",
    "populate_company_info": "Populate Company Info",
    "import_stock_prices_csv": "Import Stock Prices (CSV)",
    "update_stock_prices": "Update Stock Prices",
    "parse_taxonomy": "Parse Taxonomy",
    "generate_financial_statements": "Generate Financial Statements",
    "generate_ratios": "Generate Ratios",
    "generate_historical_ratios": "Generate Historical Ratios",
    "generate_financial_ratios": "Generate Financial Ratios",
    "find_significant_predictors": "Find Significant Predictors",
    "Multivariate_Regression": "Multivariate Regression",
    "backtest": "Backtest Portfolio",
    "backtest_set": "Backtest Set (CSV)",
}

DEFAULT_STEPS = list(STEP_DISPLAY.keys())

STEPS_WITH_OVERWRITE: set[str] = {
    "standardize_data",
    "generate_financial_statements",
    "generate_ratios",
    "generate_historical_ratios",
    "generate_financial_ratios",
    "find_significant_predictors",
}

DEFAULT_STEP_CONFIGS: dict[str, dict] = {
    "get_documents": {"startDate": "", "endDate": ""},
    "download_documents": {
        "docTypeCode": "120",
        "csvFlag": "1",
        "secCode": "",
        "Downloaded": "False",
    },
    "populate_company_info": {
        "csv_file": "config/reference/EdinetcodeDlInfo.csv",
        "Target_Database": "",
    },
    "import_stock_prices_csv": {
        "Target_Database": "",
        "csv_file": "",
        "default_ticker": "",
        "default_currency": "JPY",
        "date_column": "Date",
        "price_column": "Close",
        "ticker_column": "",
        "currency_column": "",
    },
    "update_stock_prices": {
        "Target_Database": "",
    },
    "parse_taxonomy": {
        "xsd_file": "config/reference/jppfs_cor_2013-08-31.xsd",
        "Target_Database": "",
    },
    "generate_financial_statements": {
        "Source_Database": "",
        "Source_Table": "Standard_Data",
        "Target_Database": "",
        "Company_Info_Table": "",
        "Stock_Prices_Table": "",
        "Mappings_Config": "config/reference/financial_statements_mappings_config.json",
        "batch_size": 2500,
    },
    "generate_ratios": {
        "Source_Database": "",
        "Target_Database": "",
        "Formulas_Config": "config/reference/generate_ratios_formulas_config.json",
        "batch_size": 5000,
    },
    "generate_historical_ratios": {
        "Source_Database": "",
        "Target_Database": "",
        "company_batch_size": 200,
    },
    "find_significant_predictors": {
        "Source_Database": "",
        "table_name": "",
        "output_file": "data/ols_results/predictor_search_results.txt",
        "winsorize_thresholds": {"lower": 0.05, "upper": 0.95},
        "alpha": 0.05,
        "dependent_variables": [],
    },
    "Multivariate_Regression": {
        "Source_Database": "",
        "Output": "data/ols_results/ols_results_summary.txt",
        "winsorize_thresholds": {"lower": 0.05, "upper": 0.95},
        "SQL_Query": "",
    },
    "backtest": {
        "Source_Database": "",
        "PerShare_Table": "PerShare",
        "Financial_Statements_Table": "FinancialStatements",
        "start_date": "2023-01-01",
        "end_date": "2025-12-31",
        "portfolio": {},
        "benchmark_ticker": "",
        "output_file": "data/backtest_results/backtest_report.txt",
        "risk_free_rate": 0.0,
    },
    "backtest_set": {
        "Source_Database": "",
        "PerShare_Table": "PerShare",
        "Financial_Statements_Table": "FinancialStatements",
        "csv_file": "",
        "benchmark_ticker": "",
        "output_dir": "data/backtest_set_results",
        "risk_free_rate": 0.0,
        "initial_capital": 0.0,
    },
}


def read_env() -> dict[str, str]:
    if ENV_PATH.exists():
        return dict(dotenv_values(str(ENV_PATH)))
    return {}


def write_env(key: str, value: str):
    if not ENV_PATH.exists():
        ENV_PATH.touch()
    set_key(str(ENV_PATH), key, value)


def load_app_state() -> dict:
    try:
        if APP_STATE_PATH.exists():
            with open(APP_STATE_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"recent_databases": []}


def save_app_state(state: dict):
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        with open(APP_STATE_PATH, "w") as f:
            json.dump(state, f, indent=2)
    except OSError:
        pass


def load_run_config() -> dict:
    try:
        if RUN_CONFIG_PATH.exists():
            with open(RUN_CONFIG_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"run_steps": {s: False for s in DEFAULT_STEPS}}


def save_run_config(cfg: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def list_saved_setups() -> list[str]:
    SAVED_SETUPS_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(f.stem for f in SAVED_SETUPS_DIR.glob("*.json"))


def save_named_setup(name: str, cfg: dict) -> Path:
    SAVED_SETUPS_DIR.mkdir(parents=True, exist_ok=True)
    path = SAVED_SETUPS_DIR / f"{name}.json"
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)
    return path


def load_named_setup(name: str) -> dict:
    with open(SAVED_SETUPS_DIR / f"{name}.json") as f:
        return json.load(f)


def build_steps(run_cfg: dict) -> list[list]:
    steps: list[list] = []
    run_steps = run_cfg.get("run_steps", {}) or {}

    for name, val in run_steps.items():
        if isinstance(val, dict):
            steps.append([name, bool(val.get("enabled", False)), bool(val.get("overwrite", False))])
        else:
            steps.append([name, bool(val), False])

    # Add any newly introduced default steps that may be missing from older saved configs.
    existing = {s[0] for s in steps}
    for s in DEFAULT_STEPS:
        if s not in existing:
            steps.append([s, False, False])

    if not steps:
        steps = [[s, False, False] for s in DEFAULT_STEPS]

    return steps


def build_step_configs(run_cfg: dict) -> dict[str, dict]:
    step_configs: dict[str, dict] = {}
    for sname in STEP_CONFIG_KEY:
        cfg_key = STEP_CONFIG_KEY[sname]
        loaded = run_cfg.get(cfg_key, {})
        step_configs[sname] = loaded if loaded else copy.deepcopy(DEFAULT_STEP_CONFIGS.get(sname, {}))
    return step_configs


def build_current_config(steps: list[list], step_configs: dict[str, dict], env: dict[str, str]) -> dict:
    cfg: dict = {}
    cfg["run_steps"] = {
        name: {"enabled": enabled, "overwrite": overwrite}
        for name, enabled, overwrite in steps
    }
    for sname, cfg_key in STEP_CONFIG_KEY.items():
        scfg = step_configs.get(sname)
        if scfg:
            cfg[cfg_key] = scfg

    ratios = env.get("FINANCIAL_RATIOS_CONFIG_PATH", "")
    if ratios:
        cfg["financial_ratios_config_path"] = ratios
    return cfg
