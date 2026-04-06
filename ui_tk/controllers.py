"""Thin controller adapters between the UI and backend modules.

Persistence paths are kept stable so existing saved setups remain
backward-compatible.
"""

import copy
import json
import os
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from dotenv import dotenv_values, set_key


# ── Path helpers (kept stable for saved setup compatibility) ────────────

def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parents[1]


BASE_DIR = _base_dir()
ENV_PATH = BASE_DIR / ".env"
CONFIG_DIR = BASE_DIR / "config"
STATE_DIR = CONFIG_DIR / "state"
RUN_CONFIG_PATH = STATE_DIR / "run_config.json"
UI_PIPELINE_PATH = STATE_DIR / "ui_pipeline.json"
SAVED_SETUPS_DIR = STATE_DIR / "saved_setups"
APP_STATE_PATH = STATE_DIR / "app_state.json"
EXAMPLES_DIR = CONFIG_DIR / "examples"


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


# ── Step field registry ─────────────────────────────────────────────────
#
# Each step declares exactly which fields it needs.  The UI reads this
# registry to render only the relevant inputs.
#
# field_type values:
#   "str"       – single-line text entry
#   "num"       – single-line entry (stored as int/float)
#   "text"      – multi-line text area
#   "json"      – multi-line text area with JSON serialisation
#   "database"  – database file picker
#   "file"      – generic file picker
#   "portfolio" – interactive portfolio grid

@dataclass
class StepField:
    """Metadata for a single step-config field."""
    key: str
    field_type: str
    default: object = ""
    label: str | None = None      # defaults to *key* when ``None``
    filetypes: list[tuple[str, str]] | None = None  # for "file" picker
    height: int = 3               # for "text" / "json" areas

    @property
    def display_label(self) -> str:
        return self.label if self.label is not None else self.key


STEP_FIELD_DEFINITIONS: dict[str, list[StepField]] = {
    "get_documents": [
        StepField("startDate", "str"),
        StepField("endDate", "str"),
        StepField("Target_Database", "database"),
    ],
    "download_documents": [
        StepField("docTypeCode", "str", default="120"),
        StepField("csvFlag", "str", default="1"),
        StepField("Downloaded", "str", default="False"),
        StepField("Target_Database", "database"),
    ],
    "populate_company_info": [
        StepField("csv_file", "file",
                  default="config/reference/companyinfo.csv"),
        StepField("Target_Database", "database"),
    ],
    "import_stock_prices_csv": [
        StepField("Target_Database", "database"),
        StepField("csv_file", "file"),
        StepField("default_ticker", "str"),
        StepField("default_currency", "str", default="JPY"),
        StepField("date_column", "str", default="Date"),
        StepField("price_column", "str", default="Close"),
        StepField("ticker_column", "str"),
        StepField("currency_column", "str"),
    ],
    "update_stock_prices": [
        StepField("Target_Database", "database"),
    ],
    "parse_taxonomy": [
        StepField("xsd_file", "file",
                  default="config/reference/jppfs_cor_2013-08-31.xsd",
                  filetypes=[("XSD files", "*.xsd"), ("All files", "*.*")]),
        StepField("Target_Database", "database"),
    ],
    "generate_financial_statements": [
        StepField("Source_Database", "database"),
        StepField("Source_Table", "str", default="financialData_full"),
        StepField("Target_Database", "database"),
        StepField("Company_Info_Table", "str"),
        StepField("Stock_Prices_Table", "str"),
        StepField("Mappings_Config", "file",
                  default="config/reference/financial_statements_mappings_config.json"),
        StepField("batch_size", "num", default=2500),
    ],
    "generate_ratios": [
        StepField("Source_Database", "database"),
        StepField("Target_Database", "database"),
        StepField("Formulas_Config", "file",
                  default="config/reference/generate_ratios_formulas_config.json"),
        StepField("batch_size", "num", default=5000),
    ],
    "generate_historical_ratios": [
        StepField("Source_Database", "database"),
        StepField("Target_Database", "database"),
        StepField("company_batch_size", "num", default=200),
    ],
    "Multivariate_Regression": [
        StepField("Source_Database", "database"),
        StepField("Output", "file",
                  default="data/ols_results/ols_results_summary.txt"),
        StepField("winsorize_thresholds", "json",
                  default={"lower": 0.05, "upper": 0.95},
                  label="winsorize_thresholds (JSON)"),
        StepField("SQL_Query", "text", height=6),
    ],
    "backtest": [
        StepField("Source_Database", "database"),
        StepField("PerShare_Table", "str", default="PerShare"),
        StepField("Financial_Statements_Table", "str",
                  default="FinancialStatements"),
        StepField("start_date", "str", default="2023-01-01"),
        StepField("end_date", "str", default="2025-12-31"),
        StepField("benchmark_ticker", "str"),
        StepField("output_file", "str",
                  default="data/backtest_results/backtest_report.txt"),
        StepField("risk_free_rate", "num", default=0.0),
        StepField("portfolio", "portfolio", default={}),
    ],
    "backtest_set": [
        StepField("Source_Database", "database"),
        StepField("PerShare_Table", "str", default="PerShare"),
        StepField("Financial_Statements_Table", "str",
                  default="FinancialStatements"),
        StepField("csv_file", "file",
                  filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]),
        StepField("benchmark_ticker", "str"),
        StepField("output_dir", "str",
                  default="data/backtest_set_results"),
        StepField("risk_free_rate", "num", default=0.0),
        StepField("initial_capital", "num", default=0.0),
    ],
}


def _build_defaults_from_fields() -> dict[str, dict]:
    """Derive DEFAULT_STEP_CONFIGS from the field registry."""
    defaults: dict[str, dict] = {}
    for step_name, fields in STEP_FIELD_DEFINITIONS.items():
        defaults[step_name] = {
            f.key: copy.deepcopy(f.default) for f in fields
        }
    return defaults


DEFAULT_STEP_CONFIGS: dict[str, dict] = _build_defaults_from_fields()


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
    """Write the CLI run config to ``config/state/run_config.json``.

    This file is consumed by the CLI / headless execution path
    (``src.orchestrator.run``).  The UI does **not** read it at startup;
    use :func:`save_ui_pipeline` / :func:`load_ui_pipeline` for that.
    """
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def load_run_config() -> dict:
    """Load the CLI run config, or return defaults."""
    try:
        if RUN_CONFIG_PATH.exists():
            with open(RUN_CONFIG_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"run_steps": {s: {"enabled": False, "overwrite": False}
                          for s in ALL_STEP_NAMES}}


# ── UI pipeline persistence (separate from CLI run_config) ──────────────

def save_ui_pipeline(cfg: dict):
    """Persist the current UI pipeline state to ``config/state/ui_pipeline.json``."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(UI_PIPELINE_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def load_ui_pipeline() -> dict:
    """Load the UI pipeline state, or return defaults."""
    try:
        if UI_PIPELINE_PATH.exists():
            with open(UI_PIPELINE_PATH) as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {"run_steps": {s: {"enabled": False, "overwrite": False}
                          for s in ALL_STEP_NAMES}}


# ── Template generation ─────────────────────────────────────────────────

def generate_template_run_config(dest: Path | None = None) -> Path:
    """Generate a template ``run_config.json`` with all steps and their
    default field values.  Returns the path of the written file.

    Parameters
    ----------
    dest : Path, optional
        Where to write the template.  Defaults to
        ``config/examples/run_config.template.json``.
    """
    if dest is None:
        EXAMPLES_DIR.mkdir(parents=True, exist_ok=True)
        dest = EXAMPLES_DIR / "run_config.template.json"

    cfg: dict = {}
    cfg["run_steps"] = {
        s: {"enabled": False, "overwrite": False}
        for s in ALL_STEP_NAMES
    }
    for sname in ALL_STEP_NAMES:
        cfg_key = STEP_CONFIG_KEY.get(sname)
        if cfg_key:
            cfg[cfg_key] = copy.deepcopy(DEFAULT_STEP_CONFIGS.get(sname, {}))

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        json.dump(cfg, f, indent=2)
    return dest


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


# ---------------------------------------------------------------------------
# SCREENING
# ---------------------------------------------------------------------------

SAVED_SCREENINGS_DIR = STATE_DIR / "saved_screenings"
SCREENING_HISTORY_PATH = STATE_DIR / "screening_history.jsonl"


def screening_get_metrics(db_path: str) -> dict[str, list[str]]:
    """Return available screening metrics from the database."""
    from src.screening import get_available_metrics
    return get_available_metrics(db_path)


def screening_get_periods(db_path: str) -> list[str]:
    """Return available period years from the database."""
    from src.screening import get_available_periods
    return get_available_periods(db_path)


def screening_run(db_path, criteria, columns, period, sort_by, sort_order):
    """Run a screening query and return results as a DataFrame."""
    from src.screening import run_screening
    return run_screening(db_path, criteria, columns, period, sort_by, sort_order)


def screening_export(df, output_path) -> str:
    """Export screening results to CSV."""
    from src.screening import export_screening_to_csv
    return export_screening_to_csv(df, output_path)


def screening_save(name, criteria, columns, period):
    """Save screening criteria to disk."""
    from src.screening import save_screening_criteria
    return save_screening_criteria(
        name, criteria, columns, period, str(SAVED_SCREENINGS_DIR)
    )


def screening_load(name) -> dict:
    """Load saved screening criteria."""
    from src.screening import load_screening_criteria
    return load_screening_criteria(name, str(SAVED_SCREENINGS_DIR))


def screening_list() -> list[str]:
    """List saved screening names."""
    from src.screening import list_saved_screenings
    return list_saved_screenings(str(SAVED_SCREENINGS_DIR))


def screening_delete(name) -> None:
    """Delete a saved screening."""
    from src.screening import delete_screening_criteria
    return delete_screening_criteria(name, str(SAVED_SCREENINGS_DIR))


def screening_save_history(entry) -> None:
    """Append a screening history entry."""
    from src.screening import save_screening_history
    return save_screening_history(entry, str(SCREENING_HISTORY_PATH))


def screening_load_history() -> list[dict]:
    """Load screening run history."""
    from src.screening import load_screening_history
    return load_screening_history(str(SCREENING_HISTORY_PATH))
