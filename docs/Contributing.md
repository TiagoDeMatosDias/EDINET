# Contributing to EDINET

First off, thank you for considering contributing to EDINET! It's people like you that make this project great.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue or assessing patches and features.

## Ground Rules

- Ensure that the project remains standardized and modular. New sections should be able to be added or removed without significant refactoring.
- Keep files organized. Each type of file should be in its relevant folder:
    - Documentation in `docs/`
    - Tests in `tests/`
    - Configuration files in `config/`
    - Source code in `src/`
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

### Pull Requests

- Create a separate branch for each feature or bug fix.
- Provide a clear and descriptive title for your pull request.
- In the pull request description, explain the changes you have made and why.
- Link any relevant issues in the pull request description.

We look forward to your contributions!
