#!/usr/bin/env python3
"""Check that local Markdown links under docs resolve inside the repository."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = PROJECT_ROOT / "docs"
LINK_PATTERN = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


def _local_target(raw_target: str) -> str | None:
    target = raw_target.strip().strip("<>")
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None
    return unquote(parsed.path)


def broken_links(markdown_file: Path) -> list[str]:
    """Return missing local targets referenced by one Markdown file."""
    failures = []
    content = markdown_file.read_text(encoding="utf-8")
    for match in LINK_PATTERN.finditer(content):
        target = _local_target(match.group(1))
        if target in (None, ""):
            continue
        candidate = (markdown_file.parent / target).resolve(strict=False)
        try:
            candidate.relative_to(PROJECT_ROOT)
        except ValueError:
            failures.append(f"escapes repository: {target}")
            continue
        if not candidate.exists():
            failures.append(target)
    return failures


def main() -> int:
    failures = []
    for markdown_file in sorted(DOCS_ROOT.rglob("*.md")):
        if "archive" in markdown_file.relative_to(DOCS_ROOT).parts:
            continue
        for target in broken_links(markdown_file):
            relative = markdown_file.relative_to(PROJECT_ROOT)
            failures.append(f"{relative}: {target}")
    if failures:
        print("Broken documentation links:", file=sys.stderr)
        print("\n".join(f"  {failure}" for failure in failures), file=sys.stderr)
        return 1
    print("Documentation links resolve")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
