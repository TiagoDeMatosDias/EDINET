#!/usr/bin/env python3
"""Render the compatibility requirements file from pyproject.toml."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = PROJECT_ROOT / "pyproject.toml"
REQUIREMENTS = PROJECT_ROOT / "requirements.txt"


def render_requirements() -> str:
    """Return the canonical compatibility requirements content."""
    with PYPROJECT.open("rb") as handle:
        metadata = tomllib.load(handle)
    project = metadata["project"]
    optional = project.get("optional-dependencies", {})
    sections = (
        ("Runtime", project.get("dependencies", [])),
        ("Development", optional.get("dev", [])),
        ("Build", optional.get("build", [])),
    )
    lines = [
        "# Generated compatibility input. pyproject.toml is authoritative.",
        "# Regenerate/check with: python scripts/sync_requirements.py [--check]",
    ]
    for heading, dependencies in sections:
        lines.extend(("", f"# {heading}", *dependencies))
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail instead of rewriting when requirements.txt has drifted.",
    )
    args = parser.parse_args()
    expected = render_requirements()
    current = REQUIREMENTS.read_text(encoding="utf-8") if REQUIREMENTS.exists() else ""
    if current == expected:
        print("requirements.txt is synchronized")
        return 0
    if args.check:
        print("requirements.txt differs from pyproject.toml", file=sys.stderr)
        return 1
    REQUIREMENTS.write_text(expected, encoding="utf-8", newline="\n")
    print("requirements.txt updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
