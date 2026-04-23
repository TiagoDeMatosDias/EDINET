"""
Logging utility for the EDINET application.

This module provides centralized logging functionality that:
- Writes all output to a timestamped log file
- Archives old logs to a folder
- Duplicates all printed output to both console and log file
"""

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path


class LogSetup:
    """Sets up logging for the application with file and console handlers."""

    def __init__(self, log_dir="logs", archive_dir="logs/archive"):
        """
        Initialize logging setup.

        Args:
            log_dir: Directory to store current logs (default: logs/)
            archive_dir: Directory to store archived logs (default: logs/archive/)
        """
        self.log_dir = Path(log_dir)
        self.archive_dir = Path(archive_dir)

        # Create directories if they don't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def setup_logging(self):
        """
        Configure logging with both file and console output.

        Returns:
            logging.Logger: Configured logger instance
        """
        # Archive old logs from logs directory (not archive)
        self._archive_existing_logs()

        # Create timestamped log filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"run_{timestamp}.log"
        log_filepath = self.log_dir / log_filename

        # Configure root logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # Remove any existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create formatter
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # File handler
        file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Suppress verbose DEBUG logs from third-party libraries.
        # statsmodels in particular is extremely chatty at DEBUG level and
        # will generate gigabytes of log output when running hundreds/thousands
        # of OLS regressions (e.g. find_significant_predictors).
        for noisy_logger in (
            "chardet.charsetprober",
            "statsmodels",
            "matplotlib",
            "PIL",
            "urllib3",
        ):
            logging.getLogger(noisy_logger).setLevel(logging.WARNING)

        return logger, log_filepath

    def _archive_existing_logs(self):
        """
        Move old log files from logs/ directory to logs/archive/.
        Keeps only the current session's logs in the main directory.
        """
        for log_file in self.log_dir.glob("run_*.log"):
            if log_file.is_file():
                try:
                    archive_path = self.archive_dir / log_file.name
                    shutil.move(str(log_file), str(archive_path))
                except Exception as e:
                    print(f"Warning: Could not archive log file {log_file.name}: {e}")


def setup_logging(log_dir="logs", archive_dir="logs/archive"):
    """
    Convenience function to set up logging.

    Args:
        log_dir: Directory to store current logs
        archive_dir: Directory to store archived logs

    Returns:
        tuple: (logger, log_filepath)
    """
    log_setup = LogSetup(log_dir, archive_dir)
    return log_setup.setup_logging()
