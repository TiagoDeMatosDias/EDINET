"""
Logging utility for the Shade Research application.

This module provides centralized logging functionality that:
- Writes all output to a rotating log file under the project's ``logs/`` folder
- Duplicates all output to both console and log file

Design notes
------------
The log directory is resolved to an **absolute path anchored at the project
root**, so the server always logs to ``<project_root>/logs/`` no matter which
working directory it is launched from (``main.py``, ``scripts/share.sh``, a
frozen exe, a test harness, ...).

The log file has a **stable name** (``server.log``) and is managed by a
:class:`logging.handlers.RotatingFileHandler`.  A stable name means a running
server's log is never moved out from under it (the previous timestamped-file +
archive design let a *new* launcher run archive the file a *still-running*
server was writing to, which made logging appear to stop).  Rotation keeps the
folder bounded for multi-day runs instead of growing a single file without
limit.
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path

# Rotation policy: cap each log file and keep a small number of backups so the
# logs/ folder stays bounded even for multi-day runs.  10 MiB x 6 files keeps
# at most ~60 MiB of logs on disk.
_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MiB per file
_LOG_BACKUP_COUNT = 5

# Stable log file name.  A single, always-present file is what guarantees the
# "the server always logs to logs/" property: there is no per-run file that can
# be renamed, archived, or left behind in a different working directory.
DEFAULT_LOG_FILENAME = "server.log"


def _find_project_root() -> Path:
    """Return the project root directory.

    - PyInstaller frozen exe: the folder that contains the exe.
    - Plain Python script: walks up from this module to find the repo root
      (identified by ``config/`` and ``src/orchestrator/`` directories).
    """
    if getattr(sys, "frozen", False):
        return Path(os.path.dirname(sys.executable))

    current = Path(__file__).resolve().parent
    for _ in range(5):
        parent = current.parent
        if parent == current:
            break
        current = parent
        if (current / "config").is_dir() and (current / "src" / "orchestrator").is_dir():
            return current
    # Fallback: three levels up from src/utilities/
    return Path(__file__).resolve().parent.parent.parent


def _resolve_log_dir(log_dir: "str | os.PathLike | None") -> Path:
    """Resolve the log directory to an absolute path anchored at the project root.

    Relative paths are interpreted relative to the project root, so the server
    always logs to ``<project_root>/logs/`` regardless of the current working
    directory.  Absolute paths are used as-is (useful for tests and for
    operators who want to redirect logs elsewhere).
    """
    if log_dir is None:
        return _find_project_root() / "logs"
    path = Path(log_dir)
    if path.is_absolute():
        return path
    return _find_project_root() / path


class LogSetup:
    """Sets up logging for the application with a rotating file and console output."""

    def __init__(
        self,
        log_dir: "str | os.PathLike | None" = "logs",
        log_filename: str = DEFAULT_LOG_FILENAME,
    ):
        """
        Initialize logging setup.

        Args:
            log_dir: Directory to store logs.  Relative paths are anchored at
                the project root so the server always logs to
                ``<project_root>/logs/``.  Defaults to ``logs``.
            log_filename: Stable log file name.  Defaults to ``server.log``.
        """
        self.log_dir = _resolve_log_dir(log_dir)
        self.log_filename = log_filename

        # Create the directory if it doesn't exist.
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def setup_logging(self):
        """
        Configure logging with both file and console output.

        Returns:
            tuple: (logging.Logger, Path) — the configured root logger and the
            active log file path.
        """
        # Configure root logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # Remove any existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create formatter
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        log_filepath = self.log_dir / self.log_filename

        # Rotating file handler: keeps the log bounded and the file name stable,
        # so a running server's log is never moved out from under it.
        file_handler = logging.handlers.RotatingFileHandler(
            log_filepath,
            maxBytes=_LOG_MAX_BYTES,
            backupCount=_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Suppress verbose DEBUG logs from third-party libraries.
        for noisy_logger in (
            "chardet.charsetprober",
            "matplotlib",
            "PIL",
            "urllib3",
        ):
            logging.getLogger(noisy_logger).setLevel(logging.WARNING)

        # Make uvicorn's loggers (including the per-request access log) propagate
        # to the root logger so they are captured by the file handler.  Uvicorn
        # installs its own console handler on these loggers; without propagation
        # the file log only shows infrequent application-level messages and looks
        # "silent" during normal traffic — the "logging stopped" symptom.
        # ``reconfigure_uvicorn_logging`` is called again from the app lifespan
        # (after uvicorn has finished configuring itself) to be safe.
        reconfigure_uvicorn_logging()

        return logger, log_filepath


def reconfigure_uvicorn_logging() -> None:
    """Point uvicorn's loggers at the root logger so the file handler captures them.

    Uvicorn configures ``uvicorn``, ``uvicorn.error`` and ``uvicorn.access`` with
    a console-only handler.  We keep that handler (so the console still shows
    requests) but also enable propagation so the same records reach the root
    logger's file handler.  This is idempotent and safe to call more than once
    (e.g. from the app lifespan, after uvicorn has finished configuring).
    """
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(name)
        uv_logger.propagate = True


def setup_logging(
    log_dir: "str | os.PathLike | None" = "logs",
    archive_dir: "str | os.PathLike | None" = None,
    log_filename: str = DEFAULT_LOG_FILENAME,
):
    """
    Convenience function to set up logging.

    Args:
        log_dir: Directory to store logs.  Relative paths are anchored at the
            project root so the server always logs to ``<project_root>/logs/``.
        archive_dir: Retained for backward compatibility.  Rotation replaces the
            old archive-on-startup behaviour, so this argument is ignored.
        log_filename: Stable log file name.  Defaults to ``server.log``.

    Returns:
        tuple: (logger, log_filepath)
    """
    log_setup = LogSetup(log_dir, log_filename)
    return log_setup.setup_logging()
