"""Thin controller adapters between the UI and backend modules.

All persistence paths mirror the existing Flet UI so saved setups are
backward-compatible.
"""

import copy
import json
import os
import sys
import threading
from pathlib import Path
from typing import Callable

from dotenv import dotenv_values, set_key


# ── Path helpers (same logic as ui/pages/pipeline/persistence.py) ───────

def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parents[1]


BASE_DIR = _base_dir()
ENV_PATH = BASE_DIR / ".env"
STATE_DIR = BASE_DIR / "config" / "state"
RUN_CONFIG_PATH = STATE_DIR / "run_config.json"
SAVED_SETUPS_DIR = STATE_DIR / "saved_setups"
APP_STATE_PATH = STATE_DIR / "app_state.json"


# ── Step catalogue (mirrors orchestrator + persistence.py) ──────────────

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
    "Multivariate_Regression": "Multivariate_Regression_config",
    "backtest": "backtesting_config",
    "backtest_set": "backtest_set_config",
}

STEP_DISPLAY: dict[str, str] = {
    "get_documents": "Get Documents",
    "download_documents": "Download Documents",
    "populate_company_info": "Populate Company Info",
    "import_stock_prices_csv": "Import Stock Prices (CSV)",
    "update_stock_prices": "Update Stock Prices",
    "parse_taxonomy": "Parse Taxonomy",
    "generate_financial_statements": "Generate Financial Statements",
    "generate_ratios": "Generate Ratios",
    "generate_historical_ratios": "Generate Historical Ratios",
    "Multivariate_Regression": "Multivariate Regression",
    "backtest": "Backtest Portfolio",
    "backtest_set": "Backtest Set (CSV)",
}

ALL_STEP_NAMES: list[str] = list(STEP_DISPLAY.keys())

STEPS_WITH_OVERWRITE: set[str] = {
    "generate_financial_statements",
    "generate_ratios",
    "generate_historical_ratios",
}

DEFAULT_STEP_CONFIGS: dict[str, dict] = {
    "get_documents": {
        "startDate": "",
        "endDate": "",
        "Target_Database": "",
    },
    "download_documents": {
        "docTypeCode": "120",
        "csvFlag": "1",
        "secCode": "",
        "Downloaded": "False",
        "Target_Database": "",
    },
    "populate_company_info": {
        "csv_file": "config/reference/companyinfo.csv",
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
        "Source_Table": "financialData_full",
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


# ── Pipeline execution ──────────────────────────────────────────────────

def run_pipeline(
    steps: list[dict],
    config_dict: dict,
    on_step_start: Callable[[str], None] | None = None,
    on_step_done: Callable[[str], None] | None = None,
    on_step_error: Callable[[str, Exception], None] | None = None,
    cancel_event: threading.Event | None = None,
):
    """Adapter: build a Config from *config_dict* and call the orchestrator."""
    from config import Config
    from src import orchestrator

    config = Config.from_dict(config_dict)
    orchestrator.run_pipeline(
        steps=steps,
        config=config,
        on_step_start=on_step_start,
        on_step_done=on_step_done,
        on_step_error=on_step_error,
        cancel_event=cancel_event,
    )


# ── Setup / config persistence ──────────────────────────────────────────

def list_setups() -> list[str]:
    """Return sorted list of saved setup names."""
    SAVED_SETUPS_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(f.stem for f in SAVED_SETUPS_DIR.glob("*.json"))


def load_setup(name: str) -> dict:
    """Load a named setup from ``config/state/saved_setups/{name}.json``."""
    with open(SAVED_SETUPS_DIR / f"{name}.json") as f:
        return json.load(f)


def save_setup(name: str, setup_data: dict) -> Path:
    """Save a named setup to ``config/state/saved_setups/{name}.json``."""
    SAVED_SETUPS_DIR.mkdir(parents=True, exist_ok=True)
    path = SAVED_SETUPS_DIR / f"{name}.json"
    with open(path, "w") as f:
        json.dump(setup_data, f, indent=2)
    return path


def save_run_config(cfg: dict):
    """Write the active config to ``config/state/run_config.json``."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def load_run_config() -> dict:
    """Load the active run config, or return defaults."""
    try:
        if RUN_CONFIG_PATH.exists():
            with open(RUN_CONFIG_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"run_steps": {s: {"enabled": False, "overwrite": False}
                          for s in ALL_STEP_NAMES}}


# ── API key ─────────────────────────────────────────────────────────────

def get_api_key() -> str:
    if ENV_PATH.exists():
        vals = dotenv_values(str(ENV_PATH))
        return vals.get("API_KEY", "")
    return ""


def save_api_key(key: str):
    if not ENV_PATH.exists():
        ENV_PATH.touch()
    set_key(str(ENV_PATH), "API_KEY", key)


# ── App state ───────────────────────────────────────────────────────────

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


# ── Helper: build config dict from UI state ─────────────────────────────

def build_config_dict(steps: list, step_configs: dict) -> dict:
    """Build a ``run_config.json``-compatible dict from UI state.

    *steps*: list of ``[name, overwrite]`` pairs.
    *step_configs*: ``{step_name: {field: value, ...}}``.
    """
    cfg: dict = {}
    cfg["run_steps"] = {
        name: {"enabled": True, "overwrite": overwrite}
        for name, overwrite in steps
    }
    for sname, cfg_key in STEP_CONFIG_KEY.items():
        scfg = step_configs.get(sname)
        if scfg:
            cfg[cfg_key] = scfg
    return cfg


def build_steps_from_config(run_cfg: dict) -> list:
    """Convert ``run_steps`` from a config dict into ``[[name, overwrite], ...]``.

    Only steps marked as enabled (or truthy) are included.
    """
    steps = []
    run_steps = run_cfg.get("run_steps", {}) or {}
    for name, val in run_steps.items():
        if isinstance(val, dict):
            if val.get("enabled", False):
                steps.append([name, bool(val.get("overwrite", False))])
        else:
            if val:
                steps.append([name, False])
    return steps


def build_step_configs_from_config(run_cfg: dict) -> dict:
    """Build per-step config dicts with defaults filled in.

    Only keys that appear in the step's default config are kept from the
    loaded data, so stale or mis-assigned keys in saved JSON are silently
    discarded rather than shown to the user.
    """
    step_configs: dict[str, dict] = {}
    for sname in STEP_CONFIG_KEY:
        cfg_key = STEP_CONFIG_KEY[sname]
        loaded = run_cfg.get(cfg_key, {}) or {}
        defaults = copy.deepcopy(DEFAULT_STEP_CONFIGS.get(sname, {}))
        # Whitelist: only keep loaded values whose keys exist in defaults
        filtered = {k: v for k, v in loaded.items() if k in defaults}
        step_configs[sname] = {**defaults, **filtered}
    return step_configs


def get_default_config_for_step(step_name: str) -> dict:
    """Return deep copy of the default config for *step_name*."""
    return copy.deepcopy(DEFAULT_STEP_CONFIGS.get(step_name, {}))
