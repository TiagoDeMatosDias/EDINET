# Logging System

## Overview

This project now has a comprehensive logging system that automatically:

- **Captures all output** to timestamped log files
- **Archives old logs** to a dedicated archive folder
- **Logs with timestamps** for easy tracking and debugging
- **Displays critical info** on console while storing detailed logs to files

## Directory Structure

```
logs/
├── run_YYYYMMDD_HHMMSS.log    # Current session log (created per run)
└── archive/
    └── run_YYYYMMDD_HHMMSS.log # Archived logs from previous runs
```

## How It Works

1. **Automatic Setup**: The logging system is automatically initialized in `main.py`
2. **File Creation**: Each run creates a new timestamped log file (e.g., `run_20260225_175213.log`)
3. **Auto-archiving**: Previous log files are automatically moved to `logs/archive/` when the application starts
4. **Console + File**: All messages are logged to both console and file

## Using the Logger

Import the logger in any module and use it:

```python
import logging

logger = logging.getLogger(__name__)

# Different log levels
logger.debug("Detailed debug information")
logger.info("General informational message")
logger.warning("Warning about potential issues")
logger.error("Error occurred", exc_info=True)  # exc_info=True includes traceback
```

## Log Levels

- **DEBUG**: Detailed diagnostic information
- **INFO**: General informational messages (displayed in console)
- **WARNING**: Warning messages
- **ERROR**: Error messages with exception details

## Console vs File

- **Console**: Shows INFO level and above only (cleaner output)
- **File**: Shows DEBUG level and above (complete record)

## Git Ignore

The `logs/` directory is excluded from git tracking to prevent log files from being committed.

## Example Log Output

```
2026-02-25 17:52:13 - INFO - __main__ - ================================================================================
2026-02-25 17:52:13 - INFO - __main__ - APPLICATION START
2026-02-25 17:52:13 - INFO - __main__ - ================================================================================
2026-02-25 17:52:13 - INFO - orchestrator - Starting Program
2026-02-25 17:52:13 - INFO - orchestrator - Loading Config
2026-02-25 17:52:13 - INFO - orchestrator - Getting all documents with metadata...
...
2026-02-25 17:55:42 - INFO - orchestrator - Program Ended
2026-02-25 17:55:42 - INFO - __main__ - ================================================================================
2026-02-25 17:55:42 - INFO - __main__ - APPLICATION COMPLETED SUCCESSFULLY
2026-02-25 17:55:42 - INFO - __main__ - ================================================================================
```
