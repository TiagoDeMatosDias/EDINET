"""Saved-screen and screening-history persistence."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

logger = logging.getLogger(__name__)


def normalize_screening_date(value: str | None) -> str | None:
    """Return a canonical ISO date, accepting blank input as no date."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    try:
        parsed = date.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("screening_date must use YYYY-MM-DD") from exc
    if parsed.isoformat() != normalized:
        raise ValueError("screening_date must use YYYY-MM-DD")
    return normalized


def _sanitize_name(name: str) -> str:
    safe_name = re.sub(r"[^\w\s-]", "", name).strip()
    if not safe_name:
        raise ValueError("Screening name must not be empty")
    return safe_name


def _display_name(file_path: Path) -> str:
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        display_name = str(data.get("name", "")).strip()
        return display_name or file_path.stem
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return file_path.stem


def _find_path(name: str, directory: Path) -> Path | None:
    target = str(name).strip()
    if not target or not directory.exists():
        return None
    return next(
        (
            path
            for path in sorted(directory.glob("*.json"))
            if _display_name(path) == target
        ),
        None,
    )


def _next_path(directory: Path, safe_name: str) -> Path:
    candidate = directory / f"{safe_name}.json"
    suffix = 2
    while candidate.exists():
        candidate = directory / f"{safe_name}-{suffix}.json"
        suffix += 1
    return candidate


def _write_json_atomic(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def save_screening_criteria(
    name: str,
    criteria: list[dict],
    columns: list[str],
    period: str | None,
    save_dir: str,
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
    computed_columns: list[dict] | None = None,
    screening_date: str | None = None,
) -> Path:
    """Persist a saved screen atomically and retain its point-in-time date."""
    directory = Path(save_dir)
    directory.mkdir(parents=True, exist_ok=True)
    display_name = str(name).strip()
    path = _find_path(display_name, directory) or _next_path(
        directory,
        _sanitize_name(display_name),
    )
    _write_json_atomic(
        path,
        {
            "name": display_name,
            "criteria": criteria,
            "columns": columns,
            "period": period,
            "ranking_algorithm": ranking_algorithm,
            "ranking_rules": ranking_rules or [],
            "computed_columns": computed_columns or [],
            "screening_date": normalize_screening_date(screening_date),
        },
    )
    logger.info("Saved screening criteria '%s' to %s", display_name, path)
    return path


def load_screening_criteria(name: str, save_dir: str) -> dict:
    directory = Path(save_dir)
    path = _find_path(name, directory)
    if path is None:
        raise FileNotFoundError(f"Screening '{name}' not found in {directory}")
    data = json.loads(path.read_text(encoding="utf-8"))
    data.setdefault("screening_date", None)
    return data


def list_saved_screenings(save_dir: str) -> list[str]:
    directory = Path(save_dir)
    if not directory.exists():
        return []
    return sorted(
        (_display_name(path) for path in directory.glob("*.json")),
        key=str.casefold,
    )


def delete_screening_criteria(name: str, save_dir: str) -> None:
    directory = Path(save_dir)
    path = _find_path(name, directory)
    if path is None:
        raise FileNotFoundError(f"Screening '{name}' not found in {directory}")
    path.unlink()
    logger.info("Deleted screening criteria '%s'", name)


def save_screening_history(entry: dict, history_path: str) -> None:
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(entry)
    payload.setdefault("timestamp", datetime.now().isoformat())
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_screening_history(history_path: str) -> list[dict]:
    path = Path(history_path)
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            if line.strip():
                entries.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("Skipping malformed history line")
    entries.reverse()
    return entries
