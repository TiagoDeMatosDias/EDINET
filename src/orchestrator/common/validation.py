from __future__ import annotations

import json
from typing import Any

from config import Config

from . import StepDefinition


def has_config_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return value not in (None, [], {})


def normalize_pipeline_steps(
    config: Config,
    steps: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if steps is not None:
        normalized_steps: list[dict[str, Any]] = []
        for step in steps:
            if not isinstance(step, dict):
                raise RuntimeError("Each pipeline step must be a dict.")
            step_name = step.get("name")
            if not step_name:
                raise RuntimeError("Each pipeline step requires a non-empty 'name'.")
            normalized_steps.append(
                {
                    "name": str(step_name),
                    "overwrite": bool(step.get("overwrite", False)),
                }
            )
        return normalized_steps

    run_steps = config.get("run_steps", {}) or {}
    if not isinstance(run_steps, dict):
        raise RuntimeError("config.run_steps must be a dict of pipeline steps.")

    normalized_steps = []
    for step_name, step_val in run_steps.items():
        if isinstance(step_val, dict):
            if step_val.get("enabled", False):
                normalized_steps.append(
                    {
                        "name": str(step_name),
                        "overwrite": bool(step_val.get("overwrite", False)),
                    }
                )
        elif bool(step_val):
            normalized_steps.append({"name": str(step_name), "overwrite": False})
    return normalized_steps


def apply_step_config_defaults(
    config: Config,
    normalized_steps: list[dict[str, Any]],
    *,
    step_definitions: dict[str, StepDefinition],
) -> None:
    for step in normalized_steps:
        raw_step_name = step["name"]
        definition = step_definitions.get(raw_step_name)
        if definition is None:
            continue

        cfg_name = definition.resolved_config_key
        step_cfg_raw = config.settings.get(cfg_name)
        if step_cfg_raw is None:
            config.settings[cfg_name] = definition.build_default_config()
            continue
        if not isinstance(step_cfg_raw, dict):
            continue

        default_cfg = definition.build_default_config()
        merged_cfg = {**default_cfg, **step_cfg_raw}
        if merged_cfg != step_cfg_raw:
            config.settings[cfg_name] = merged_cfg


def _validate_step_field_value(field_type: str, value: Any) -> str | None:
    if field_type == "num":
        try:
            float(value)
        except (TypeError, ValueError):
            return "must be numeric"
        return None

    if field_type == "json":
        if isinstance(value, str):
            try:
                json.loads(value)
            except json.JSONDecodeError:
                return "must be valid JSON"
        return None

    return None


def validate_pipeline_input(
    config: Config,
    normalized_steps: list[dict[str, Any]],
    *,
    step_definitions: dict[str, StepDefinition],
) -> None:
    missing_map: dict[str, list[str]] = {}
    invalid_map: dict[str, list[str]] = {}
    unknown_steps: list[str] = []
    duplicate_steps: list[str] = []
    seen_steps: set[str] = set()

    for step in normalized_steps:
        raw_step_name = step["name"]
        definition = step_definitions.get(raw_step_name)
        if definition is None:
            unknown_steps.append(raw_step_name)
            continue
        if raw_step_name in seen_steps:
            duplicate_steps.append(raw_step_name)
            continue
        seen_steps.add(raw_step_name)

        for key in definition.required_keys:
            if not has_config_value(config.get(key)):
                missing_map.setdefault(key, []).append(raw_step_name)

        cfg_name = definition.resolved_config_key
        step_cfg_raw = config.settings.get(cfg_name, {}) or {}
        if not isinstance(step_cfg_raw, dict):
            invalid_map.setdefault(cfg_name, []).append("must be a JSON object / dict")
            continue
        step_cfg = step_cfg_raw

        for field in definition.required_input_fields:
            if not has_config_value(step_cfg.get(field.key)):
                missing_key = f"{cfg_name}.{field.key}"
                missing_map.setdefault(missing_key, []).append(raw_step_name)

        for field in definition.input_fields:
            if not has_config_value(step_cfg.get(field.key)):
                continue
            error = _validate_step_field_value(field.field_type, step_cfg.get(field.key))
            if error:
                invalid_key = f"{cfg_name}.{field.key}"
                invalid_map.setdefault(invalid_key, []).append(f"{raw_step_name}: {error}")

    if unknown_steps:
        raise RuntimeError(f"Unknown orchestrator step(s): {', '.join(sorted(unknown_steps))}")

    if duplicate_steps:
        raise RuntimeError(f"Duplicate orchestrator step(s): {', '.join(sorted(duplicate_steps))}")

    if missing_map or invalid_map:
        lines: list[str] = []
        if missing_map:
            lines.append("The following required settings are missing from .env / config:")
            for key, steps_needing in sorted(missing_map.items()):
                lines.append(f"  • {key}  (needed by: {', '.join(steps_needing)})")

        if invalid_map:
            if lines:
                lines.append("")
            lines.append("The following step config values are invalid:")
            for key, problems in sorted(invalid_map.items()):
                lines.append(f"  • {key}  ({'; '.join(problems)})")

        lines.append("")
        lines.append("Set them in the step configuration dialogs or add them to the config / .env files.")
        raise RuntimeError("\n".join(lines))