"""Runtime paths and artifact limits for reproducible reports."""

from pathlib import Path

from src.utilities.runtime_paths import report_root
from src.web_app.security import get_settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_ROOT = report_root()
MAX_REPORT_BYTES = get_settings().max_report_artifact_bytes
REPORT_ROOT.mkdir(parents=True, exist_ok=True)
