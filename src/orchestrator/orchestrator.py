import logging
import threading
from typing import Any, Callable

from config import Config

from .common import build_step_registry
from .common import StepDefinition
from .common.validation import apply_step_config_defaults, normalize_pipeline_steps, validate_pipeline_input

logger = logging.getLogger(__name__)

STEP_HANDLERS: dict[str, Callable] = {}
STEP_CANONICAL_NAMES: dict[str, str] = {}
STEP_DEFINITIONS: dict[str, StepDefinition] = {}
DISCOVERED_STEP_MODULES: tuple[str, ...] = ()


def refresh_step_registry() -> None:
    """Rebuild orchestrator step registries from discovered step modules."""
    global STEP_HANDLERS
    global STEP_CANONICAL_NAMES
    global STEP_DEFINITIONS
    global DISCOVERED_STEP_MODULES

    (
        STEP_HANDLERS,
        STEP_CANONICAL_NAMES,
        STEP_DEFINITIONS,
        DISCOVERED_STEP_MODULES,
    ) = build_step_registry()


def canonical_step_name(step_name: str) -> str:
    """Return the canonical name for *step_name*, resolving any aliases."""
    return STEP_CANONICAL_NAMES.get(step_name, step_name)


def list_available_steps() -> list[dict[str, Any]]:
    """Return discovered steps with metadata from each step definition."""
    return [
        STEP_DEFINITIONS[step_name].to_dict()
        for step_name in sorted(STEP_DEFINITIONS)
    ]


def list_discovered_step_modules() -> list[str]:
    """Return the dotted module paths currently loaded for step discovery."""
    return list(DISCOVERED_STEP_MODULES)


refresh_step_registry()


def _coerce_config(config: Config | dict[str, Any] | None) -> Config:
    if config is None:
        raise ValueError(
            "No configuration provided. All pipeline configuration must be "
            "supplied via the API or programmatically via Config.from_dict()."
        )
    if isinstance(config, Config):
        return config
    if isinstance(config, dict):
        return Config.from_dict(config)
    raise TypeError("config must be a Config instance or a dict.")


def _resolve_enabled_steps(
    config: Config,
    steps: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    return normalize_pipeline_steps(config, steps=steps)


def validate_config(config, enabled_steps: list[str]) -> None:
    """Validate that all required settings exist for the enabled steps.

    Raises ``RuntimeError`` with a detailed message when settings are missing.
    """
    try:
        normalized_steps = [
            {"name": step_name, "overwrite": False}
            for step_name in enabled_steps
        ]
        apply_step_config_defaults(
            config,
            normalized_steps,
            step_definitions=STEP_DEFINITIONS,
            canonical_names=STEP_CANONICAL_NAMES,
        )
        validate_pipeline_input(
            config,
            normalized_steps,
            step_definitions=STEP_DEFINITIONS,
            canonical_names=STEP_CANONICAL_NAMES,
        )
    except RuntimeError as exc:
        logger.error(str(exc))
        raise


def validate_input(
    config: Config | dict[str, Any] | None,
    steps: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Validate a provided pipeline config and return normalized enabled steps."""
    config_obj = _coerce_config(config)
    normalized_steps = _resolve_enabled_steps(config_obj, steps=steps)
    apply_step_config_defaults(
        config_obj,
        normalized_steps,
        step_definitions=STEP_DEFINITIONS,
        canonical_names=STEP_CANONICAL_NAMES,
    )
    validate_pipeline_input(
        config_obj,
        normalized_steps,
        step_definitions=STEP_DEFINITIONS,
        canonical_names=STEP_CANONICAL_NAMES,
    )
    return normalized_steps


def execute_step(step_name: str, config, overwrite: bool = False):
    """Execute a single orchestration step by name."""
    handler = STEP_HANDLERS.get(step_name)
    if handler is None:
        logger.warning("Unknown step: %s", step_name)
        return None
    return handler(config, overwrite=overwrite)


def run(
    config: Config | dict[str, Any] | None = None,
    steps: list[dict[str, Any]] | None = None,
    on_step_start: Callable[[str], None] | None = None,
    on_step_done: Callable[[str], None] | None = None,
    on_step_error: Callable[[str, Exception], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    """Run the provided pipeline config, or load the saved config when omitted."""
    logger.info("Starting Program")
    logger.info("Loading Config")

    config_obj = _coerce_config(config)
    enabled_steps = validate_input(config_obj, steps=steps)

    logger.info("Steps to execute (in order): %s", [step["name"] for step in enabled_steps])
    for step in enabled_steps:
        if cancel_event and cancel_event.is_set():
            logger.info("Pipeline cancelled by user.")
            return

        step_name = step["name"]
        overwrite = step.get("overwrite", False)

        if on_step_start:
            on_step_start(step_name)
        try:
            execute_step(step_name, config_obj, overwrite=overwrite)
            if on_step_done:
                on_step_done(step_name)
        except Exception as exc:
            logger.error("Error executing step '%s': %s", step_name, exc, exc_info=True)
            if on_step_error:
                on_step_error(step_name, exc)
            raise

    logger.info("Program Ended")


__all__ = [
    "list_available_steps",
    "run",
    "validate_input",
]