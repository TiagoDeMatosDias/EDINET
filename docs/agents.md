# Agents Guide

Last updated: 2026-03-31

Purpose:
- Short guide for automation agents (or human reviewers) working in this repository.
- Where to find authoritative documentation and the rules to follow when editing docs.

---

## Primary docs (start here)
- [docs/Application Details.md](docs/Application%20Details.md) — "Python Source File Reference" (living document). Primary per-file API reference: responsibilities, signatures, Inputs/Outputs, Calls/Dependencies.
- [docs/Readme.md](docs/Readme.md) — high-level project overview and goals.
- [docs/RUNNING.md](docs/RUNNING.md) — how to run the app locally and common commands.
- [docs/LOGGING.md](docs/LOGGING.md) — logging configuration and conventions.
- [docs/Contributing.md](docs/Contributing.md) — contribution workflow, PR guidelines, code style.
- [docs/CHANGELOG.md](docs/CHANGELOG.md) — release notes and notable changes.

Also check:
- `src/` and `ui/` for implementation (source to document).
- `tests/` for usage examples and expected behaviours.

---

## When updating docs — hard rules
- Update documentation in the same PR/commit that changes code.
- Keep `Last updated` current (YYYY-MM-DD) at top of each living doc.
- Maintain ordering by file path.
- Per-file format (must follow exactly):
  - `def name(args) -> ReturnType`
    - Purpose: one-line summary.
    - Inputs: list/describe important inputs.
    - Output: return type/shape.
    - Calls/Dependencies: comma-separated list of function names called inside this function (only the names; no extra text).
- For classes: document each public method as a separate function entry under the same file section (include `def ClassName.method(...) -> ...` or `def method(...) -> ...` under a `Class: ClassName` subheading).
- Calls/Dependencies must list only the called function names discovered in the function body (use static analysis/AST to extract). Do not list modules, SQL statements, or logger calls unless they are functions invoked in the body.
- Do not duplicate implementation details — keep Purpose concise.

---

## Practical guidance for agents
- Use the source (`src/`, `ui/`) to extract exact function signatures and called functions; prefer AST parsing over regex when possible.
- When extracting calls, record the attribute name (for `obj.method()`) or function name (for direct calls). De-duplicate and sort alphabetically for readability.
- For ambiguous types or dynamic code, prefer the exact textual signature from source; do not invent type annotations.
- If a function is moved/renamed, update its documentation path and all callers' Calls/Dependencies lists in the same PR.
- If you cannot determine a call target statically (dynamic dispatch, getattr, etc.), leave the Calls/Dependencies entry minimal and add a short note in the PR description for a human reviewer.
- Ask for human review when you change behaviour descriptions, update public APIs, or when automatic extraction misses complex call sites.

---

## Quick commands
- Run tests:
```powershell
python -m pytest -q
```
- Capture UI screenshots (all views × both themes → `data/mockups/screenshots/`):
```powershell
python -m pytest tests/test_ui_screenshots.py -v
```
- Capture with a prefix for before/after comparison:
```python
from tests.test_ui_screenshots import capture_all_views
capture_all_views(themes=["dark"], prefix="before_")
```
- Quick AST-based scan (example) to list functions and called names:
```powershell
python - <<'PY'
import ast, pathlib
for f in pathlib.Path('src').rglob('*.py'):
    src=f.read_text(encoding='utf-8')
    tree=ast.parse(src)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            name = node.name
            if isinstance(node, ast.ClassDef):
                for m in node.body:
                    if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        calls=set()
                        for n in ast.walk(m):
                            if isinstance(n, ast.Call):
                                if isinstance(n.func, ast.Attribute):
                                    calls.add(n.func.attr)
                                elif isinstance(n.func, ast.Name):
                                    calls.add(n.func.id)
                        if calls:
                            print(f"{f} :: {node.name}.{m.name} -> {', '.join(sorted(calls))}")
            else:
                calls=set()
                for n in ast.walk(node):
                    if isinstance(n, ast.Call):
                        if isinstance(n.func, ast.Attribute):
                            calls.add(n.func.attr)
                        elif isinstance(n.func, ast.Name):
                            calls.add(n.func.id)
                if calls:
                    print(f"{f} :: {name} -> {', '.join(sorted(calls))}")
PY
```

---

## Review checklist (before creating PR)
- Docs entry exists/updated for each changed file/method.
- `Last updated` date updated.
- Calls/Dependencies lists only function names and matches source (use AST to verify).
- Class methods documented individually.
- PR description calls out any dynamic call sites or unresolved references for reviewer attention.

## Feature Development checklist

Prior to starting work on a new feature, ensure the following documentation is in place:
- [ ] A high-level design document outlining the feature's purpose, scope, and implementation plan.
- [ ] A detailed implementation plan that includes the specific files and functions that will be modified or added, along with their intended responsibilities and interactions.
- [ ] Implementation of the feature in the codebase, following the established coding standards and practices.
- [ ] Updated `docs/Readme.md` if the feature significantly changes the project's goals or architecture.
- [ ] A new section in `docs/Application Details.md` for any new files or modules introduced by the feature, following the established format.
- [ ] A new section in `docs/RUNNING.md` if the feature introduces new commands, configuration options, or setup steps.
- [ ] Updated `docs/CHANGELOG.md` with a summary of the feature and its impact on the project.
- [ ] Comprehensive tests for the new feature, with examples of expected behavior, added to the `tests/` directory.
