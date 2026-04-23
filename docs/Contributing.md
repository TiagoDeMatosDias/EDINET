# Contributing to EDINET

First off, thank you for considering contributing to EDINET! It's people like you that make this project great.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue or assessing patches and features.

## Quick Checklist: Documentation Updates

Use this checklist when your change affects behavior, UI, workflows, or architecture:

- Update user-facing docs impacted by the change (for example `docs/Readme.md` and `docs/RUNNING.md`)
- Update technical docs when implementation details changed (especially `docs/Application Details.md`)
- Update contributing guidance if contributor workflows changed (for example screenshot/dev loops in `docs/Contributing.md`)
- If UI changed, refresh screenshots (`python -m pytest tests/test_ui_screenshots.py -v`) and update assets in `docs/images/`
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
    - UI code in `ui_tk/`
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

All Python code in this project follows a consistent commenting style. The standards below are derived from `src/regression_analysis.py`, which serves as the reference implementation.

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

### UI Development — Screenshot Review Loop

The project includes a screenshot capture test suite (`tests/test_ui_screenshots.py`) that enables a visual review development workflow.  This is especially useful when working with an AI coding assistant:

1. **Capture** — run the screenshot tests to save PNGs of every view.
2. **Review** — inspect the screenshots (or have an AI agent review them) for visual issues.
3. **Fix** — implement corrections based on what you see.
4. **Repeat** — re-run the capture and compare.

#### Running the screenshot tests

```powershell
# Capture all views in both themes (saves to data/mockups/screenshots/)
python -m pytest tests/test_ui_screenshots.py -v
```

Screenshots are saved to `data/mockups/screenshots/` with filenames like `home_dark.png`, `orchestrator_light.png`, etc.

#### Updating screenshots used in documentation

When UI changes are substantial, update documentation screenshots in the same pull request.

1. Capture fresh screenshots:

```powershell
python -m pytest tests/test_ui_screenshots.py -v
```

2. Copy selected images from `data/mockups/screenshots/` to `docs/images/` using clear, stable names.

```powershell
Copy-Item data/mockups/screenshots/home_dark.png docs/images/ui-home-dark.png -Force
Copy-Item data/mockups/screenshots/orchestrator_dark.png docs/images/ui-orchestrator-dark.png -Force
Copy-Item data/mockups/screenshots/data_dark.png docs/images/ui-data-dark.png -Force
```

3. Update markdown links in docs (for example, in `docs/Readme.md`) to point to the new files.

4. Remove obsolete images that are no longer referenced to keep `docs/images/` clean.

5. Verify all image links resolve correctly in markdown preview before opening the PR.

Notes:

- Prefer PNG files for UI screenshots.
- Keep image dimensions consistent across related screenshots when possible.
- Include both dark/light captures when the change affects theme-specific visuals.

#### Using the reusable API in scripts or notebooks

The module exposes a `capture_all_views()` helper that can be called outside of pytest:

```python
from tests.test_ui_screenshots import capture_all_views

# Capture a "before" snapshot with a filename prefix
capture_all_views(themes=["dark"], prefix="before_")

# ... make UI changes ...

# Capture an "after" snapshot
capture_all_views(themes=["dark"], prefix="after_")
```

Parameters:

| Parameter   | Default              | Description                                                    |
|-------------|----------------------|----------------------------------------------------------------|
| `views`     | All (`Home`, `Orchestrator`, `Data`) | List of view names to capture.                   |
| `themes`    | Current theme only   | `["dark"]`, `["light"]`, or `["dark", "light"]`.               |
| `geometry`  | `"1100x750"`         | Window size for the capture.                                   |
| `settle_ms` | `800`                | Milliseconds to wait for rendering before capture.             |
| `prefix`    | `""`                 | Filename prefix (e.g. `"pre_refactor_"`) for before/after comparisons. |

#### Requirements

- A real display (the tests auto-skip in headless environments).
- `Pillow` (already in `requirements.txt`) for `ImageGrab`.
- `customtkinter` (already in `requirements.txt`) for rounded button rendering.

#### Tips

- Use the `prefix` parameter to create timestamped or labelled snapshots for side-by-side comparison.
- When working with an AI assistant, ask it to `view_image` the captured screenshots to identify visual regressions, alignment issues, or colour problems.
- The tests create a fresh `App` instance for each capture, so they always reflect the latest code.

---

### Adding a New Pipeline Step

The pipeline is designed so that adding a new step requires changes in exactly three places, with no UI-specific branching needed:

#### 1. Orchestrator step package (`src/orchestrator/<step_name>/`)

Create a new step package with an `__init__.py` and a same-named implementation module. The orchestrator discovers it automatically, so no core registry edit is needed:

```python
from src.orchestrator.common import StepDefinition


def run_my_new_step(config, overwrite=False):
    step_cfg = config.get("my_new_step_config", {})
    # ... call the relevant module with explicit params ...

STEP_DEFINITION = StepDefinition(
    name="my_new_step",
    handler=run_my_new_step,
    required_config_fields=(("my_new_step_config", "Target_Database"),),
)
```

Example package layout:

```text
src/orchestrator/my_new_step/
├── __init__.py
└── my_new_step.py
```

Declare any required top-level keys with `required_keys` and any required step-config fields with `required_config_fields` inside the `StepDefinition`.

#### 2. Step catalogue & field registry (`ui_tk/controllers.py`)

Register the step in the catalogue constants and declare its config fields:

```python
# Add to STEP_CONFIG_KEY
STEP_CONFIG_KEY["my_new_step"] = "my_new_step_config"

# Add to STEP_DISPLAY
STEP_DISPLAY["my_new_step"] = "My New Step"

# Declare fields in STEP_FIELD_DEFINITIONS
STEP_FIELD_DEFINITIONS["my_new_step"] = [
    StepField("Source_Database", "database"),
    StepField("output_file", "file", default="data/output.txt"),
    StepField("batch_size", "num", default=1000),
]
```

The `DEFAULT_STEP_CONFIGS` dict is derived automatically from the field definitions — there is nothing else to update.

If the step supports overwrite, add it to `STEPS_WITH_OVERWRITE`.

#### 3. Verify

Run the test suite to ensure nothing is broken:

```powershell
python -m pytest tests/ -v
```

The UI config panel will automatically render the correct inputs for the new step based on the field definitions. No changes to the UI page code are needed.

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
