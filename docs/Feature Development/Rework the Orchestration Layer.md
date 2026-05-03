# Orchestration Rework

**Status: COMPLETED (2026-04-03)**

## Original Problem

The current orchestration layer is a mess with a lot of duplicated code and a lot of code that is not used anymore, and weird choices and dependencies (why is the edinet a class that needs to be initiated? Why not just use functions? Why is the configuration tightly coupled with the class? Why are there so many nested function calls?). 

The goal of this rework is to clean up the orchestration layer and make it more maintainable.

## Requirements

- Every underlying component (edinet, database, etc.) should be a separate module with a clear interface. The orchestration layer should just call these modules and not have any logic of its own.
- The underlying components should be stateless and should not have any side effects. They should just take input and return output. No global state or class variables should be used.
- The orchestration layer should be a simple function that takes input and returns output. It should not have any logic of its own and should just call the underlying components in the correct order.
- The configuration should be passed as an argument to the orchestration function and should not be tightly coupled with the underlying components. The underlying components should just take the configuration as an argument and not have any knowledge of where it came from.
- The orchestration layer should be easy to test and should not have any dependencies on external services or databases. The underlying components should be easy to mock and should not have any side effects that could affect the tests.
- The orchestration layer should be easy to extend and should not have any hardcoded logic. The underlying components should be easy to replace and should not have any dependencies on each other.
- The orchestration layer should be easy to read and understand. The underlying components should have clear and concise interfaces and should not have any unnecessary complexity.
- The orchestration layer should be easy to debug and should not have any hidden state or side effects. The underlying components should be easy to debug and should not have any hidden state or side effects.

## What Was Done

Current structure note:
- The original `src/orchestrator.py` entrypoint has since been converted into the `src/orchestrator/` package.
- Step registration now lives in package-per-step modules directly under `src/orchestrator/`, extracted workflow bodies live under `src/orchestrator/services/`, and the shared helper mixins used by those services now live under `src/orchestrator/processor/`.
- `src/data_processing.py` remains as a backward-compatible facade/subclass rather than the primary orchestrator implementation surface.

### 1. `src/orchestrator.py` — Full rewrite
- **Step handler pattern**: Replaced the monolithic `if/elif` chain with a `STEP_HANDLERS` dict mapping step names to dedicated handler functions (`_step_get_documents`, `_step_download_documents`, etc.).
- **No shared state**: `run()` and `run_pipeline()` no longer pre-create shared `Edinet` or `data` instances. Each handler creates what it needs with explicit params.
- **`execute_step(step_name, config, overwrite)`**: Simplified signature — dispatches to the appropriate handler from the registry.
- **`validate_config()`**: Extracted pre-flight validation into a standalone function.
- **`run()`**: Simplified — no more `edinet`/`data` parameters.

### 2. `src/edinet_api.py` — Decoupled from Config
- `Edinet.__init__` now takes explicit parameters: `base_url`, `api_key`, `db_path`, `raw_docs_path`, `doc_list_table`, `company_info_table`, `taxonomy_table`.
- Removed `import config as c` — no Config singleton dependency.

### 3. `src/data_processing.py` — Decoupled from Config
- `data.__init__` no longer reads Config. The class is now a stateless namespace.
- All public methods require their database paths to be passed explicitly (no fallback to `self.DB_PATH` or `self.config`).
- `parse_edinet_taxonomy()` gained an optional `db_path` parameter for when no connection is provided.

### 4. `src/utils.py` — Decoupled from Config
- `generateURL(docID, base_url, api_key, doctype=None)` — takes explicit params instead of a Config object.

### 5. Tests updated
- `test_orchestrator.py` — Tests for `run_pipeline`, `execute_step` dispatch, `validate_config`, and `Config.from_dict`.
- `test_edinet_api.py` — Create `Edinet` with explicit params, no Config mocking.
- `test_utils.py` — Pass explicit params to `generateURL`, no Config mocking.
- `test_data_processing.py` — No changes needed (already bypassed `__init__`).

### Results
- **153 tests pass** (all existing + new tests).
- **No Config dependency** in any module except `orchestrator.py` and `config.py` itself.
- **Easy to extend**: Add a new step by writing a handler function and adding it to `STEP_HANDLERS`.
- **Easy to test**: Mock any handler or module function directly.
