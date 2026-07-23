# Contributing

Updated: 2026-07-22

## Environment

Use Python 3.12 or 3.13 in `.venv3`, Node.js 22, and npm 10. Install from the authoritative `pyproject.toml` and lockfile:

```powershell
py -3.13 -m venv .venv3
.\.venv3\Scripts\python.exe -m pip install -e ".[dev,build]"
Set-Location frontend-v2
npm ci
Set-Location ..
```

`requirements.txt` is a generated compatibility input. After changing dependencies, run `python scripts/sync_requirements.py`, then verify it with `--check`.

## Verification

Run the same bounded stages used by CI:

```powershell
.\.venv3\Scripts\python.exe -B scripts\verify.py
```

For a focused change, repeat `--stage` with one or more of `unit`, `integration`, `frontend-test`, `frontend-lint`, `frontend-build`, `requirements`, `documentation`, `static-ruff`, `static-mypy`, or `package-check`. Every stage has a hard timeout and pytest workspaces are removed after each run; do not replace the verifier with an unbounded wrapper.

Before review:

- add or update tests for changed behavior;
- run the relevant focused stage, then the complete verifier when practical;
- update `docs/Application Details.md` for Python API/module changes;
- update frontend/API contract types together;
- run `scripts/check_docs.py` after documentation changes;
- do not include operator databases, runtime state, uploads, logs, API keys, or generated exports.

## Code structure

- Keep public functions cohesive and preferably below 80 lines; delegate named operations rather than growing route handlers.
- Route handlers validate/authorize, call a service, and serialize a typed response.
- Use `src.orchestrator.common.sqlite` for managed connections and explicit write transactions.
- Treat API paths, uploads, and filenames as untrusted. Use `PathPolicy` and server-owned output names.
- Log tracebacks server-side and return the shared safe error envelope.
- Preserve stable facades when moving implementations (`src.api.router:app`, `src.screening`, Portfolio schema/model imports).
- Do not perform a repository-wide formatting rewrite alongside behavioral changes.

## Adding a pipeline step

Create `src/orchestrator/<step_name>/__init__.py` and a same-named implementation module. Export one `STEP_DEFINITION`; discovery supplies the API/frontend metadata.

```python
from src.orchestrator.common import StepDefinition, StepFieldDefinition


def run_my_step(config, *, overwrite=False, context=None):
    items = load_items(config)
    for index, item in enumerate(items):
        if context is not None:
            context.checkpoint()
            context.report_progress(index, len(items), f"Processing {index + 1}")
        process(item, overwrite=overwrite)


STEP_DEFINITION = StepDefinition(
    name="my_step",
    handler=run_my_step,
    display_name="My Step",
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)
```

`__init__.py` should re-export only the definition:

```python
from .my_step import STEP_DEFINITION

__all__ = ["STEP_DEFINITION"]
```

Long/network/batch work must accept the optional execution context and checkpoint at safe transaction boundaries. Do not persist configuration secrets or upload bodies in job state. Add discovery, validation, success, first-failure, and cancellation tests.

## React work

- Put top-level workspaces in `frontend-v2/src/features/` and reusable primitives in `src/components/`.
- Use the shared API client and TanStack Query for server state.
- Update `frontend-v2/src/api/types.ts` with backend contract changes.
- Add a Vitest test and, for route/API changes, update `tests/unit/test_openapi_contract.py`.
- Save documentation screenshots under `docs/images/` at a consistent viewport.

## Pull requests

Keep changes reviewable and explain behavior, migrations, security implications, and verification results. A migration must document backup/recovery behavior. Do not commit on behalf of the operator unless explicitly asked.
