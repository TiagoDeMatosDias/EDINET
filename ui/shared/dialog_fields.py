import copy
import json

import flet as ft


def build_fields(cfg: dict, prefix: str = "") -> list[tuple]:
    """Return [(dotted_key | None, Control), …] for a config dict."""
    fields: list[tuple] = []
    for key, val in cfg.items():
        fk = f"{prefix}.{key}" if prefix else key
        if isinstance(val, dict):
            fields.append((None, ft.Text(key, weight=ft.FontWeight.BOLD, size=13)))
            fields.extend(build_fields(val, fk))
        elif isinstance(val, list):
            text = "\n".join(str(v) for v in val)
            fields.append((
                fk,
                ft.TextField(
                    label=key,
                    value=text,
                    dense=True,
                    multiline=True,
                    min_lines=2,
                    max_lines=6,
                ),
            ))
        else:
            fields.append((fk, ft.TextField(label=key, value=str(val), dense=True)))
    return fields


def read_fields(fields: list[tuple], original: dict) -> dict:
    """Read edited values from form fields back into a config dict."""
    result = copy.deepcopy(original)
    for key_path, ctrl in fields:
        if key_path is None or not isinstance(ctrl, ft.TextField):
            continue
        raw = ctrl.value
        parts = key_path.split(".")
        target = result
        for p in parts[:-1]:
            target = target.setdefault(p, {})
        last = parts[-1]
        orig_val = target.get(last)

        if isinstance(orig_val, bool):
            target[last] = raw.strip().lower() in ("true", "1", "yes")
        elif isinstance(orig_val, float):
            try:
                target[last] = float(raw)
            except ValueError:
                target[last] = raw
        elif isinstance(orig_val, int):
            try:
                target[last] = int(raw)
            except ValueError:
                target[last] = raw
        elif isinstance(orig_val, list):
            try:
                target[last] = json.loads(raw)
            except json.JSONDecodeError:
                target[last] = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        else:
            target[last] = raw
    return result
