import base64
import binascii
import hashlib
import inspect
import json
import logging
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import threading
from typing import Any, Callable
from uuid import uuid4

from config import Config

from .common import build_step_registry
from .common import StepDefinition
from .common.validation import apply_step_config_defaults, normalize_pipeline_steps, validate_pipeline_input

logger = logging.getLogger(__name__)

STEP_HANDLERS: dict[str, Callable] = {}
STEP_DEFINITIONS: dict[str, StepDefinition] = {}
DISCOVERED_STEP_MODULES: tuple[str, ...] = ()

_DEFAULT_UPLOAD_LIMIT = 10 * 1024 * 1024
_MANUAL_UPLOAD_ROOT = (
    Path(__file__).resolve().parents[2]
    / "config"
    / "state"
    / "manual_jobs"
)
_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_WINDOWS_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{number}" for number in range(1, 10)}
    | {f"LPT{number}" for number in range(1, 10)}
)
_OWNED_OUTPUT_NAMES = {
    "output_file": "backtest_report.txt",
    "output_dir": "backtest_set",
    "download_dir": "taxonomy_downloads",
}


class InvalidUploadError(ValueError):
    """Raised when an embedded pipeline upload is malformed."""


class UploadTooLargeError(InvalidUploadError):
    """Raised when an embedded pipeline upload exceeds its byte limit."""


def refresh_step_registry() -> None:
    """Rebuild orchestrator step registries from discovered step modules."""
    global STEP_HANDLERS
    global STEP_DEFINITIONS
    global DISCOVERED_STEP_MODULES

    (
        STEP_HANDLERS,
        STEP_DEFINITIONS,
        DISCOVERED_STEP_MODULES,
    ) = build_step_registry()


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
        )
        validate_pipeline_input(
            config,
            normalized_steps,
            step_definitions=STEP_DEFINITIONS,
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
    )
    validate_pipeline_input(
        config_obj,
        normalized_steps,
        step_definitions=STEP_DEFINITIONS,
    )
    return normalized_steps


def execute_step(
    step_name: str,
    config,
    overwrite: bool = False,
    context: Any | None = None,
):
    """Execute a single orchestration step by name."""
    handler = STEP_HANDLERS.get(step_name)
    if handler is None:
        logger.warning("Unknown step: %s", step_name)
        return None
    kwargs: dict[str, Any] = {"overwrite": overwrite}
    try:
        parameters = inspect.signature(handler).parameters.values()
        supports_context = any(
            parameter.name == "context"
            or parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
    except (TypeError, ValueError):
        supports_context = False
    if supports_context and context is not None:
        kwargs["context"] = context
    return handler(config, **kwargs)


def _safe_upload_name(value: Any) -> str:
    raw_name = str(value or "upload.bin").replace("\\", "/")
    name = _INVALID_FILENAME_CHARS.sub("_", Path(raw_name).name).strip(" .")
    if not name:
        name = "upload.bin"
    if Path(name).stem.upper() in _WINDOWS_RESERVED_NAMES:
        name = f"_{name}"
    return name[:120]


def _decode_embedded_upload(value: dict[str, Any], limit: int) -> tuple[str, bytes]:
    display_name = _safe_upload_name(value.get("filename"))
    content = value.get("content")
    if not isinstance(content, str) or not content:
        raise InvalidUploadError("Embedded file content must be non-empty base64")
    max_encoded_size = 4 * ((limit + 2) // 3)
    if len(content) > max_encoded_size:
        raise UploadTooLargeError("Embedded file exceeds the configured size limit")
    try:
        file_bytes = base64.b64decode(content, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InvalidUploadError("Embedded file content is not valid base64") from exc
    if len(file_bytes) > limit:
        raise UploadTooLargeError("Embedded file exceeds the configured size limit")
    return display_name, file_bytes


def resolve_file_uploads(
    config: dict[str, Any],
    enabled_steps: list[dict[str, Any]],
    *,
    workspace: str | Path,
    max_bytes: int = _DEFAULT_UPLOAD_LIMIT,
) -> dict[str, Any]:
    """Resolve strict embedded uploads inside an owned job workspace."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    workspace_path = Path(workspace).resolve(strict=False)
    uploads_dir = workspace_path / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    resolved = dict(config)
    manifest: list[dict[str, Any]] = []
    for step_info in enabled_steps:
        step_name = step_info["name"]
        step_def = STEP_DEFINITIONS.get(step_name)
        if not step_def:
            continue
        config_key = step_def.resolved_config_key
        step_cfg = dict(resolved.get(config_key, {}))
        modified = False
        for field in step_def.input_fields:
            if field.field_type == "file":
                value = step_cfg.get(field.key)
                if isinstance(value, dict) and value.get("content"):
                    limit = field.max_bytes or max_bytes
                    display_name, file_bytes = _decode_embedded_upload(value, limit)
                    digest = hashlib.sha256(file_bytes).hexdigest()
                    stored_name = f"{uuid4().hex}-{display_name}"
                    upload_path = uploads_dir / stored_name
                    upload_path.write_bytes(file_bytes)
                    step_cfg[field.key] = str(upload_path)
                    modified = True
                    manifest.append({
                        "step": step_name,
                        "field": field.key,
                        "display_name": display_name,
                        "stored_name": stored_name,
                        "size_bytes": len(file_bytes),
                        "sha256": digest,
                    })
                    logger.info(
                        "Resolved upload '%s' for step '%s' (%d bytes, sha256=%s)",
                        display_name,
                        step_name,
                        len(file_bytes),
                        digest[:12],
                    )
        if modified:
            resolved[config_key] = step_cfg
    if manifest:
        (workspace_path / "upload_manifest.json").write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )
    return resolved


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _authorize_pipeline_input(
    value: Any,
    *,
    roots: tuple[Path, ...],
    field_key: str,
) -> str:
    candidate = Path(str(value)).expanduser()
    if not candidate.is_absolute():
        candidate = Path(__file__).resolve().parents[2] / candidate
    if ":" in candidate.name:
        raise InvalidUploadError("Pipeline input path is not allowed")
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, FileNotFoundError) as exc:
        raise InvalidUploadError("Pipeline input file was not found") from exc
    if not resolved.is_file() or not any(_is_within(resolved, root) for root in roots):
        raise InvalidUploadError("Pipeline input path is outside allowed data roots")
    required_suffix = {"csv_file": ".csv", "xsd_file": ".xsd"}.get(field_key)
    if required_suffix and resolved.suffix.casefold() != required_suffix:
        raise InvalidUploadError("Pipeline input file type is not allowed")
    return str(resolved)


def constrain_pipeline_paths(
    config: dict[str, Any],
    enabled_steps: list[dict[str, Any]],
    *,
    workspace: str | Path,
    allowed_input_roots: tuple[str | Path, ...],
) -> dict[str, Any]:
    """Authorize local inputs and replace client output paths with owned paths."""
    workspace_path = Path(workspace).resolve(strict=False)
    roots = tuple(
        Path(root).expanduser().resolve(strict=False)
        for root in (*allowed_input_roots, workspace_path)
    )
    resolved = dict(config)
    for step_info in enabled_steps:
        step_name = str(step_info["name"])
        definition = STEP_DEFINITIONS.get(step_name)
        if definition is None:
            continue
        config_key = definition.resolved_config_key
        step_config = dict(resolved.get(config_key, {}))
        for field in definition.input_fields:
            value = step_config.get(field.key)
            if field.field_type == "file" and value:
                step_config[field.key] = _authorize_pipeline_input(
                    value,
                    roots=roots,
                    field_key=field.key,
                )
            if field.key in _OWNED_OUTPUT_NAMES:
                output_root = workspace_path / "outputs" / step_name
                output_name = _OWNED_OUTPUT_NAMES[field.key]
                step_config[field.key] = str(output_root / output_name)
        resolved[config_key] = step_config
    return resolved


def run(
    config: Config | dict[str, Any] | None = None,
    steps: list[dict[str, Any]] | None = None,
    on_step_start: Callable[[str], None] | None = None,
    on_step_done: Callable[[str], None] | None = None,
    on_step_error: Callable[[str, Exception], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    """Run a pipeline, cleaning direct-call upload workspaces on completion."""
    logger.info("Starting Program")
    logger.info("Loading Config")
    if isinstance(config, dict) and steps:
        _MANUAL_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(
            prefix="manual-",
            dir=_MANUAL_UPLOAD_ROOT,
        ) as workspace:
            resolved = resolve_file_uploads(
                config,
                steps,
                workspace=workspace,
            )
            _run_resolved(
                resolved,
                steps,
                on_step_start,
                on_step_done,
                on_step_error,
                cancel_event,
            )
        return
    _run_resolved(
        config,
        steps,
        on_step_start,
        on_step_done,
        on_step_error,
        cancel_event,
    )


def _run_resolved(
    config: Config | dict[str, Any] | None,
    steps: list[dict[str, Any]] | None,
    on_step_start: Callable[[str], None] | None,
    on_step_done: Callable[[str], None] | None,
    on_step_error: Callable[[str, Exception], None] | None,
    cancel_event: threading.Event | None,
) -> None:
    """Execute a pipeline whose embedded file values are already resolved."""

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
