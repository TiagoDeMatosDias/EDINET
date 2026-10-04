# Logging and Correlation

Updated: 2026-10-04

`main.py` initializes the root logger through `src.utilities.logger.setup_logging()`.

## Files and levels

```text
logs/
├── server.log            # active log (rotates to server.log.1 … server.log.5)
└── archive/              # legacy timestamped logs (kept for reference)
```

- The server always logs to `<project_root>/logs/server.log`, resolved to an
  **absolute path anchored at the project root** — so it lands in `logs/` no
  matter which working directory the server is launched from (`main.py`,
  `scripts/share.sh`, a frozen exe, a test harness, …).
- The log file has a **stable name** and is managed by a
  `RotatingFileHandler` (10 MiB per file, 5 backups, ~60 MiB cap).  A stable
  name means a running server's log is never moved out from under it — the
  previous timestamped-file + archive-on-startup design let a *new* launcher
  run archive the file a *still-running* server was writing to, which made
  logging appear to stop.
- Files receive DEBUG and above; the console receives INFO and above.
- Uvicorn's per-request access log is routed through the root logger, so every
  HTTP request is captured in `server.log` (not just on the console).
- Logs are runtime/operator state and are ignored by Git. Rotation bounds the
  folder automatically; operators may still apply their own retention policy.

## Request correlation

Every HTTP response carries `X-Correlation-ID`. Safe error responses use one envelope:

```json
{
  "code": "internal_error",
  "detail": "Internal server error",
  "correlation_id": "uuid"
}
```

Unexpected tracebacks are logged server-side with the correlation ID. Client-facing 500 responses do not include tracebacks, SQL, secrets, repository roots, or private database paths.

## Pipeline jobs

Pipeline transition messages include the job ID and, when relevant, the step name. Durable status, timing, progress, and bounded results live in `config/state/pipeline_jobs.db`; they are not reconstructed from logs. Retention is controlled by `EDINET_JOB_RETENTION_HOURS` and cleanup removes both expired rows and owned workspaces.

Do not log:

- EDINET/API bearer tokens;
- complete configuration dictionaries;
- embedded base64 bodies or uploaded Portfolio XML;
- unbounded step results;
- arbitrary operator file contents.

The job redaction layer removes secret-like keys and bounds serialized output before persistence. This is defense in depth; callers must still avoid placing secrets in status messages.

## Usage

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Queued pipeline job %s", job_id)
try:
    run_operation()
except Exception:
    logger.exception("Operation failed for job %s", job_id)
    raise
```

Use parameterized logger calls. Include identifiers needed to correlate work, and keep sensitive values out of both messages and exception text.
