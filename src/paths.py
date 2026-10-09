"""The application's two filesystem roots.

``app_dir`` is the folder the operator sees: the one holding
``ShadeResearch.exe`` in a packaged build, the repository root otherwise.
Everything the application writes is anchored here.

``bundle_dir`` holds the read-only files shipped inside the build (frontend
bundle, brand assets). In a one-file PyInstaller build it is the temporary
folder the executable unpacks into, which is deleted on exit, so nothing may
be written below it. Module ``__file__`` paths point there too, which is why
runtime folders must never be derived from ``__file__``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SOURCE_ROOT = Path(__file__).resolve().parents[1]


def app_dir() -> Path:
    """The folder holding the executable, or the repository root from source."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _SOURCE_ROOT


def bundle_dir() -> Path:
    """The folder holding bundled read-only resources."""
    bundled = getattr(sys, "_MEIPASS", None)
    return Path(bundled) if bundled else _SOURCE_ROOT
