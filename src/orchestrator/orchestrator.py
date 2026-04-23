import logging
import threading
import importlib
from typing import Callable

from config import Config

from .common import build_step_registry

logger = logging.getLogger(__name__)

STEP_HANDLERS: dict[str, Callable] = {}
STEP_REQUIRED_KEYS: dict[str, list[str]] = {}
STEP_REQUIRED_CONFIG_FIELDS: dict[str, list[tuple[str, str]]] = {}
STEP_CANONICAL_NAMES: dict[str, str] = {}
DISCOVERED_STEP_MODULES: tuple[str, ...] = ()


def refresh_step_registry() -> None:
    """Rebuild orchestrator step registries from discovered step modules."""
    global STEP_HANDLERS
    global STEP_REQUIRED_KEYS
    global STEP_REQUIRED_CONFIG_FIELDS
    global STEP_CANONICAL_NAMES
    global DISCOVERED_STEP_MODULES

    (
        STEP_HANDLERS,
        STEP_REQUIRED_KEYS,
        STEP_REQUIRED_CONFIG_FIELDS,
        STEP_CANONICAL_NAMES,
        DISCOVERED_STEP_MODULES,
    ) = build_step_registry()


def canonical_step_name(step_name: str) -> str:
    """Return the canonical name for *step_name*, resolving any aliases."""
    return STEP_CANONICAL_NAMES.get(step_name, step_name)


def list_available_steps() -> list[str]:
    """Return canonical step names discovered from the step modules."""
    return sorted(set(STEP_CANONICAL_NAMES.values()))


def list_discovered_step_modules() -> list[str]:
    """Return the dotted module paths currently loaded for step discovery."""
    return list(DISCOVERED_STEP_MODULES)


refresh_step_registry()


def validate_config(config, enabled_steps: list[str]) -> None:
    """Validate that all required settings exist for the enabled steps.

    Raises ``RuntimeError`` with a detailed message when settings are missing.
    """
    missing_map: dict[str, list[str]] = {}

    for raw_step_name in enabled_steps:
        step_name = canonical_step_name(raw_step_name)

        for key in STEP_REQUIRED_KEYS.get(step_name, []):
            if not config.get(key):
                missing_map.setdefault(key, []).append(raw_step_name)

        for cfg_name, field_name in STEP_REQUIRED_CONFIG_FIELDS.get(step_name, []):
            cfg = config.get(cfg_name, {}) or {}
            if not cfg.get(field_name):
                missing_key = f"{cfg_name}.{field_name}"
                missing_map.setdefault(missing_key, []).append(raw_step_name)

    if missing_map:
        lines = ["The following required settings are missing from .env / config:"]
        for key, steps_needing in sorted(missing_map.items()):
            lines.append(f"  • {key}  (needed by: {', '.join(steps_needing)})")
        lines.append("")
        lines.append("Set them in the step configuration dialogs or add them to the config / .env files.")
        msg = "\n".join(lines)
        logger.error(msg)
        raise RuntimeError(msg)


def execute_step(step_name: str, config, overwrite: bool = False):
    """Execute a single orchestration step by name."""
    handler = STEP_HANDLERS.get(step_name)
    if handler is None:
        logger.warning("Unknown step: %s", step_name)
        return None
    return handler(config, overwrite=overwrite)


def _package_execute_step(step_name: str, config, overwrite: bool = False):
    package_module = importlib.import_module(__package__)
    return package_module.execute_step(step_name, config, overwrite=overwrite)


def run() -> None:
    """Orchestrate execution based on the run config file."""
    logger.info("Starting Program")
    logger.info("Loading Config")

    config = Config()
    run_steps = config.get("run_steps", {})

    enabled_steps = []
    for step_name, step_val in run_steps.items():
        if isinstance(step_val, dict):
            if step_val.get("enabled", False):
                enabled_steps.append(step_name)
        elif bool(step_val):
            enabled_steps.append(step_name)

    validate_config(config, enabled_steps)

    logger.info("Steps to execute (in order): %s", list(run_steps.keys()))
    for step_name, step_val in run_steps.items():
        if isinstance(step_val, dict):
            is_enabled = step_val.get("enabled", False)
            overwrite = step_val.get("overwrite", False)
        else:
            is_enabled = bool(step_val)
            overwrite = False

        if is_enabled:
            try:
                _package_execute_step(step_name, config, overwrite=overwrite)
            except Exception as exc:
                logger.error("Error executing step '%s': %s", step_name, exc, exc_info=True)
        else:
            logger.debug("Step '%s' is disabled, skipping.", step_name)

    logger.info("Program Ended")


def run_pipeline(
    steps: list[dict],
    config: Config,
    on_step_start: Callable[[str], None] | None = None,
    on_step_done: Callable[[str], None] | None = None,
    on_step_error: Callable[[str, Exception], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    """Execute a list of steps in order with per-step callbacks and cancellation."""
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
            _package_execute_step(name, config, overwrite=overwrite)
            if on_step_done:
                on_step_done(name)
        except Exception as exc:
            logger.error("Error executing step '%s': %s", name, exc, exc_info=True)
            if on_step_error:
                on_step_error(name, exc)
            raise


def _build_legacy_step_wrapper(step_name: str):
    def _runner(config, overwrite=False):
        return _package_execute_step(step_name, config, overwrite=overwrite)

    _runner.__doc__ = f"Backward-compatible wrapper for the '{step_name}' step."
    return _runner


_LEGACY_STEP_WRAPPERS = {
    "_step_get_documents": "get_documents",
    "_step_download_documents": "download_documents",
    "_step_populate_company_info": "populate_company_info",
    "_step_generate_financial_statements": "generate_financial_statements",
    "_step_populate_business_descriptions_en": "populate_business_descriptions_en",
    "_step_generate_ratios": "generate_ratios",
    "_step_generate_historical_ratios": "generate_historical_ratios",
    "_step_import_stock_prices_csv": "import_stock_prices_csv",
    "_step_update_stock_prices": "update_stock_prices",
    "_step_parse_taxonomy": "parse_taxonomy",
    "_step_multivariate_regression": "Multivariate_Regression",
    "_step_backtest": "backtest",
    "_step_backtest_set": "backtest_set",
}

for _wrapper_name, _step_name in _LEGACY_STEP_WRAPPERS.items():
    globals()[_wrapper_name] = _build_legacy_step_wrapper(_step_name)


__all__ = [
    "DISCOVERED_STEP_MODULES",
    "STEP_CANONICAL_NAMES",
    "STEP_HANDLERS",
    "STEP_REQUIRED_CONFIG_FIELDS",
    "STEP_REQUIRED_KEYS",
    "canonical_step_name",
    "execute_step",
    "list_available_steps",
    "list_discovered_step_modules",
    "refresh_step_registry",
    "run",
    "run_pipeline",
    "validate_config",
]