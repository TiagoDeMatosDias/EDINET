"""Runtime paths and artifact limits for reproducible reports."""

from src.paths import reports_dir
from src.web_app.security import get_settings

REPORT_ROOT = reports_dir()
MAX_REPORT_BYTES = get_settings().max_report_artifact_bytes
REPORT_ROOT.mkdir(parents=True, exist_ok=True)
