"""Screenshot capture test suite for visual UI review.

Launches the real Tk application, navigates to each view, and saves
screenshots to ``data/mockups/screenshots/``.  These tests are intended
for developer/AI visual review — they capture, not assert layout.

Run selectively::

    pytest tests/test_ui_screenshots.py -v

The tests require a real display; they are skipped automatically in
headless environments (no DISPLAY on Linux, or ImageGrab failure).
"""

import os
import sys
import time
import tkinter as tk
from pathlib import Path

import pytest

# ── Skip when a display is unavailable ──────────────────────────────────

_HAS_DISPLAY = True
try:
    _test_root = tk.Tk()
    _test_root.withdraw()
    _test_root.destroy()
except tk.TclError:
    _HAS_DISPLAY = False

try:
    from PIL import ImageGrab
except ImportError:
    _HAS_DISPLAY = False

pytestmark = pytest.mark.skipif(
    not _HAS_DISPLAY,
    reason="No display available or Pillow not installed",
)

# ── Output directory ────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent
SCREENSHOT_DIR = ROOT_DIR / "data" / "mockups" / "screenshots"


# ── Helpers ─────────────────────────────────────────────────────────────

def _ensure_dir():
    """Create the screenshot output directory if it doesn't exist."""
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)


def _capture_window(root: tk.Tk, filename: str, *, settle_ms: int = 600):
    """Capture a screenshot of the given Tk root window.

    Parameters
    ----------
    root : tk.Tk
        The root window to capture.  Must be visible (not withdrawn).
    filename : str
        Name of the output PNG file (without directory prefix).
    settle_ms : int
        Milliseconds to wait after raising the window so rendering
        completes before capture.
    """
    _ensure_dir()

    root.lift()
    root.focus_force()
    root.attributes("-topmost", True)
    root.update_idletasks()
    root.update()

    # Give the window manager time to actually render
    time.sleep(settle_ms / 1000)
    root.update()

    x = root.winfo_rootx()
    y = root.winfo_rooty()
    w = root.winfo_width()
    h = root.winfo_height()

    img = ImageGrab.grab(bbox=(x, y, x + w, y + h))
    path = SCREENSHOT_DIR / filename
    img.save(str(path))
    print(f"  📸 saved → {path}")
    return path


def _launch_app(geometry: str = "1100x750"):
    """Create a Tk root and instantiate the App.

    Returns (root, app).  Caller is responsible for calling root.destroy().
    """
    from ui_tk.app import App

    root = tk.Tk()
    root.geometry(geometry)
    root.update()
    app = App(root)
    root.update_idletasks()
    root.update()
    return root, app


# ── Public API: reusable screenshot runner ──────────────────────────────

def capture_all_views(
    views: list[str] | None = None,
    themes: list[str] | None = None,
    geometry: str = "1100x750",
    settle_ms: int = 800,
    prefix: str = "",
) -> list[Path]:
    """Capture screenshots for every *view × theme* combination.

    Parameters
    ----------
    views : list[str] | None
        View names to capture.  ``None`` → all (Home, Orchestrator, Data).
    themes : list[str] | None
        ``["dark"]``, ``["light"]``, or ``["dark", "light"]``.
        ``None`` → capture the currently active theme only.
    geometry : str
        Root window geometry string.
    settle_ms : int
        Rendering settle time per capture.
    prefix : str
        Optional filename prefix (e.g. ``"pre_refactor_"``).

    Returns
    -------
    list[Path]
        Paths of all saved screenshots.
    """
    from ui_tk.app import VIEW_NAMES
    from ui_tk.style import toggle_theme, is_dark

    if views is None:
        views = list(VIEW_NAMES)

    root, app = _launch_app(geometry)
    paths: list[Path] = []

    try:
        themes_to_run = themes or [("dark" if is_dark() else "light")]
        for theme_name in themes_to_run:
            # Switch to the requested theme if not already active
            if (theme_name == "dark") != is_dark():
                toggle_theme(root)
                root.update_idletasks()
                root.update()
                time.sleep(0.3)

            for view in views:
                app.switch_view(view)
                root.update_idletasks()
                root.update()

                fname = f"{prefix}{view.lower()}_{theme_name}.png"
                p = _capture_window(root, fname, settle_ms=settle_ms)
                paths.append(p)
    finally:
        root.destroy()

    return paths


# ── Pytest tests ────────────────────────────────────────────────────────


class TestScreenshotCapture:
    """Capture screenshots of every main view for visual review."""

    def test_capture_home_dark(self):
        root, app = _launch_app()
        try:
            app.switch_view("Home")
            root.update_idletasks()
            root.update()
            path = _capture_window(root, "home_dark.png")
            assert path.exists()
        finally:
            root.destroy()

    def test_capture_orchestrator_dark(self):
        root, app = _launch_app()
        try:
            app.switch_view("Orchestrator")
            root.update_idletasks()
            root.update()
            path = _capture_window(root, "orchestrator_dark.png")
            assert path.exists()
        finally:
            root.destroy()

    def test_capture_data_dark(self):
        root, app = _launch_app()
        try:
            app.switch_view("Data")
            root.update_idletasks()
            root.update()
            path = _capture_window(root, "data_dark.png")
            assert path.exists()
        finally:
            root.destroy()

    def test_capture_screening_dark(self):
        root, app = _launch_app()
        try:
            app.switch_view("Screening")
            root.update_idletasks()
            root.update()
            path = _capture_window(root, "screening_dark.png")
            assert path.exists()
        finally:
            root.destroy()

    def test_capture_light_theme(self):
        """Capture all views in light theme."""
        from ui_tk.style import toggle_theme, is_dark

        root, app = _launch_app()
        try:
            # Ensure light
            if is_dark():
                toggle_theme(root)
                root.update_idletasks()
                root.update()
                time.sleep(0.3)

            for view in ("Home", "Orchestrator", "Data", "Screening"):
                app.switch_view(view)
                root.update_idletasks()
                root.update()
                path = _capture_window(root, f"{view.lower()}_light.png")
                assert path.exists()
        finally:
            # Toggle back to dark so subsequent tests start in dark
            if not is_dark():
                toggle_theme(root)
            root.destroy()


class TestScreenshotAllViews:
    """Use the batch helper to capture all views × both themes."""

    def test_capture_all_dark_and_light(self):
        paths = capture_all_views(themes=["dark", "light"])
        assert len(paths) == 8  # 4 views × 2 themes
        for p in paths:
            assert p.exists()
            assert p.stat().st_size > 0
