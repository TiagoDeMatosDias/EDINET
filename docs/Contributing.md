# Contributing to EDINET

First off, thank you for considering contributing to EDINET! It's people like you that make this project great.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue or assessing patches and features.

## Quick Checklist: Documentation Updates

Use this checklist when your change affects behavior, UI, workflows, or architecture:

- Update user-facing docs impacted by the change (for example `docs/Readme.md` and `docs/RUNNING.md`)
- Update technical docs when implementation details changed (especially `docs/Application Details.md`)
- Update contributing guidance if contributor workflows changed (for example screenshot/dev loops in `docs/Contributing.md`)
- If UI changed, capture new screenshots (see the Screenshots section below) and update assets in `docs/images/`
- Remove obsolete documentation references, commands, flags, screenshots, and files
- Verify markdown links/images render correctly in preview
- Include documentation updates in the same PR as the code change

## Ground Rules

- Ensure that the project remains standardized and modular. New sections should be able to be added or removed without significant refactoring.
- Keep files organized. Each type of file should be in its relevant folder:
    - Documentation in `docs/`
    - Tests in `tests/`
    - Configuration reference files in `config/reference/`
    - Configuration state (runtime config, saved setups) in `config/state/`
    - Configuration examples in `config/examples/`
    - Source code in `src/`
    - Web frontend code in `src/web_app/frontend/`
    - Data files in `data/`

## Your First Contribution

Unsure where to begin contributing to EDINET? You can start by looking through these beginner-friendly issues:

- Issues tagged with `good first issue`.
- Issues tagged with `help wanted`.

## Getting Started

### Code Contributions

When contributing code, please keep the following in mind:

1.  **Tests:** Create and run tests for every new function you create. All tests should pass before submitting a pull request.
2.  **Comments:** Follow the comment and docstring standards described below.
3.  **Modularity:** Keep the project standardized and modular. This allows for new sections to be added or removed without extensive rework.
4.  **File Structure:** Place files in their respective folders to maintain project organization. For example:
    - Documentation should be in the `docs/` folder.
    - Tests should be in the `tests/` folder.
    - Configuration files should be in the `config/` folder.

### Comment Standards

All Python code in this project follows a consistent commenting style. The standards below are derived from `src/orchestrator/generate_ratios/generate_ratios.py`, which serves as the reference implementation.

#### Docstrings (Google Style)

Every public function and method must have a docstring. Use **Google style** with the following sections:

- A short summary sentence on the first line.
- An extended description when the behaviour needs more explanation.
- An `Args:` section listing every parameter with its type and a brief description.
- A `Returns:` section describing the return value and its type.

```python
def my_function(param1: str, param2: int = 0) -> bool:
    """Short summary of what this function does.

    Extended description when the behaviour needs more explanation.
    This can span multiple lines.

    Args:
        param1 (str): Description of the first parameter.
        param2 (int): Description of the second parameter. Defaults to 0.

    Returns:
        bool: Description of what is returned and when.
    """
```

#### Inline Comments

- Use inline comments to explain **why** something is done, not just what the code does.
- Use `# --- Section Name ---` dividers to group related blocks of logic within a function.
- Write comments as full sentences where appropriate.

```python
# --- Data Cleaning ---
# Replace infinite values with NaN so they can be dropped.
df_cleaned = df.replace([np.inf, -np.inf], np.nan)

# Drop rows with missing values to ensure the regression runs on a complete dataset.
df_cleaned = df_cleaned.dropna(subset=all_vars)
```

#### Module-Level Section Dividers

Use a long dashed line to separate major logical sections at the module level, followed by a short description block:

```python
# ---------------------------------------------------------------------------
# SECTION NAME
# ---------------------------------------------------------------------------
# Brief description of what this section does and why it exists.
```

#### Module-Level Constants

Document module-level constants with an inline or preceding comment that explains their purpose:

```python
# Columns excluded from regression variable candidates (compared case-insensitively).
_NON_PREDICTOR_COLUMNS: frozenset[str] = frozenset({
    "index",       # row-number artefact
    "edinetcode",  # company identifier
})
```

### UI Development — Screenshots

The web workstation UI can be captured using Playwright or a standard browser screenshot tool. Launch the app with `python main.py`, open the desired page in your browser, and capture screenshots for documentation.

#### Taking screenshots

1. Launch the web server: `python main.py`
2. Open `http://127.0.0.1:8000` (or the appropriate view URL) in your browser
3. Use your browser's dev tools or a screenshot extension to capture each view
4. Save screenshots to `docs/images/` with descriptive names (e.g., `web-dashboard.png`, `web-screening.png`)
5. Update markdown image links in the README to reference the new screenshots

Notes:

- Prefer PNG files for UI screenshots.
- Keep image dimensions consistent across related screenshots when possible.
- Use a consistent browser window size (e.g., 1280×800).

---

### Adding a New Pipeline Step

The pipeline is designed so that adding a new step requires changes in exactly two places, with no UI-specific branching needed:

#### 1. Orchestrator step package (`src/orchestrator/<step_name>/`)

Create a new step package with an `__init__.py` and a same-named implementation module. The package should export only `STEP_DEFINITION`; the runtime handler stays internal to the step module. The orchestrator discovers the package automatically, so no core registry edit is needed:

```python
from src.orchestrator.common import StepDefinition, StepFieldDefinition


def run_my_new_step(config, overwrite=False):
    step_cfg = config.get("my_new_step_config", {})
    # ... call the relevant module with explicit params ...

STEP_DEFINITION = StepDefinition(
    name="my_new_step",
    handler=run_my_new_step,
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)
```

Example package layout:

```text
src/orchestrator/my_new_step/
├── __init__.py
└── my_new_step.py
```

`__init__.py` should stay minimal:

```python
from .my_new_step import STEP_DEFINITION

__all__ = ["STEP_DEFINITION"]
```

Declare any required top-level keys with `required_keys` and declare step-config fields directly in `input_fields`. Mark required step-config entries with `required=True` on the relevant `StepFieldDefinition`.

#### 2. Step-local field registry

Register the step's UI metadata and config fields directly in the step definition:

```python
from src.orchestrator.common import StepDefinition, StepFieldDefinition


STEP_DEFINITION = StepDefinition(
    name="my_new_step",
    handler=run_my_new_step,
    display_name="My New Step",
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("output_file", "file", default="data/output.txt"),
        StepFieldDefinition("batch_size", "num", default=1000),
    ),
)
```

If the step needs a custom display name, config-key override, or overwrite support, declare that metadata on `StepDefinition` as well.

The UI reads `orchestrator.list_available_steps()` and derives its menu/config widgets from the discovered step definitions — there is nothing to update in the orchestrator frontend module beyond the step package itself.

#### 3. Verify

Run the test suite to ensure nothing is broken:

```powershell
python -m pytest tests/ -v
```

The UI config panel will automatically render the correct inputs for the new step based on the orchestrator metadata. No changes to the UI page code are needed.

#### Available field types

| `field_type` | Widget | Use for |
|---|---|---|
| `"str"` | `LabeledEntry` | Single-line text (table names, tickers, dates) |
| `"num"` | `LabeledEntry` (stored as int/float) | Numeric values (batch sizes, rates) |
| `"text"` | `LabeledText` (multi-line) | Large text inputs (SQL queries) |
| `"json"` | `LabeledText` (multi-line, JSON-serialised) | Structured data (thresholds, filter dicts) |
| `"database"` | `DatabasePickerEntry` | SQLite `.db` file paths |
| `"file"` | `FilePickerEntry` | Any file path (CSV, XSD, config JSON) |
| `"portfolio"` | `PortfolioGrid` | Interactive portfolio allocation table |

All `StepField` options:

| Parameter | Default | Description |
|---|---|---|
| `key` | (required) | Config dict key |
| `field_type` | (required) | Widget type (see table above) |
| `default` | `""` | Default value for new steps |
| `label` | `None` (uses `key`) | Custom display label |
| `filetypes` | `None` | File-dialog filters for `"file"` type |
| `height` | `3` | Row count for `"text"` / `"json"` areas |

---

### Pull Requests

- Create a separate branch for each feature or bug fix.
- Provide a clear and descriptive title for your pull request.
- In the pull request description, explain the changes you have made and why.
- Link any relevant issues in the pull request description.

We look forward to your contributions!
