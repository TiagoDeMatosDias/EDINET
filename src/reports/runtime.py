"""Runtime paths and artifact limits for reproducible reports."""

from pathlib import Path

from src.web_app.security import AppSettings

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_ROOT = PROJECT_ROOT / "data" / "reports"
MAX_REPORT_BYTES = AppSettings.from_env().max_report_artifact_bytes
REPORT_ROOT.mkdir(parents=True, exist_ok=True)
