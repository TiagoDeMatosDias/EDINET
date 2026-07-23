# Logging and Correlation

Updated: 2026-07-22

`main.py` initializes the root logger through `src.utilities.logger.setup_logging()`.

## Files and levels

```text
logs/
├── run_YYYYMMDD_HHMMSS.log
└── archive/
    └── run_YYYYMMDD_HHMMSS.log
```

- A new timestamped file is created for each launcher run.
- Previous `run_*.log` files move to `logs/archive/` at startup.
- Files receive DEBUG and above; the console receives INFO and above.
- Logs are runtime/operator state and are ignored by Git. There is no automatic age-based archive deletion; operators should apply their normal retention policy.

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
