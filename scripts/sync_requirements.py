#!/usr/bin/env python3
"""Render requirements.txt and constraints.txt from pyproject.toml.

``requirements.txt`` lists direct dependencies (runtime, dev, build).
``constraints.txt`` pins their transitive dependencies from
``[tool.edinet].constraints`` so ``pip install ... -c constraints.txt`` is
reproducible without listing indirect packages as dependencies.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = PROJECT_ROOT / "pyproject.toml"
REQUIREMENTS = PROJECT_ROOT / "requirements.txt"
CONSTRAINTS = PROJECT_ROOT / "constraints.txt"


def _metadata() -> dict:
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)


def render_requirements() -> str:
    """Return the canonical compatibility requirements content."""
    project = _metadata()["project"]
    optional = project.get("optional-dependencies", {})
    sections = (
        ("Runtime", project.get("dependencies", [])),
        ("Development", optional.get("dev", [])),
        ("Build", optional.get("build", [])),
    )
    lines = [
        "# Generated compatibility input. pyproject.toml is authoritative.",
        "# Regenerate/check with: python scripts/sync_requirements.py [--check]",
        "",
        "# Pins for transitive dependencies",
        "-c constraints.txt",
    ]
    for heading, dependencies in sections:
        lines.extend(("", f"# {heading}", *dependencies))
    return "\n".join(lines) + "\n"


def render_constraints() -> str:
    """Return the transitive-dependency constraints content."""
    pins = _metadata().get("tool", {}).get("edinet", {}).get("constraints", [])
    lines = [
        "# Generated from [tool.edinet].constraints in pyproject.toml.",
        "# Regenerate/check with: python scripts/sync_requirements.py [--check]",
        "# Use with: pip install -e \".[dev]\" -c constraints.txt",
        "",
        *pins,
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail instead of rewriting when requirements.txt has drifted.",
    )
    args = parser.parse_args()
    status = 0
    for target, expected in (
        (REQUIREMENTS, render_requirements()),
        (CONSTRAINTS, render_constraints()),
    ):
        current = target.read_text(encoding="utf-8") if target.exists() else ""
        if current == expected:
            print(f"{target.name} is synchronized")
        elif args.check:
            print(f"{target.name} differs from pyproject.toml", file=sys.stderr)
            status = 1
        else:
            target.write_text(expected, encoding="utf-8", newline="\n")
            print(f"{target.name} updated")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
