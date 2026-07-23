# Building the Windows Release

Updated: 2026-07-22

Windows is the packaged target. Use Python 3.12 or 3.13, Node.js 22/npm 10, and the declared `build` dependency group.

```powershell
py -3.13 -m venv .venv3
.\.venv3\Scripts\python.exe -m pip install -e ".[build]"
```

## Canonical workflow

`EDINET.spec` is the versioned canonical PyInstaller specification. `scripts/build.py` is the only supported orchestration command:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py
```

The script, in order:

1. verifies supported Python, Node/npm, `EDINET.spec`, the frontend lockfile, and PyInstaller;
2. runs `npm ci` and the production frontend build with hard timeouts;
3. removes only the repository's exact `build/` and `dist/` directories;
4. runs PyInstaller through the active interpreter with a 600-second default cap;
5. assembles `dist/EDINET-<version>/`;
6. creates fresh empty Base, Standardized, and Portfolio SQLite databases plus a relative `database_paths.json` and `.env` template;
7. starts the packaged executable on a temporary loopback port and checks `/health`, `/`, and `/api/steps` within 45 seconds;
8. writes `dist/EDINET-<version>-Release.zip`.

Run non-mutating preflight only:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py --check
```

Override bounded stages when the build host is unusually slow:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py --command-timeout 180 --smoke-timeout 45
```

The build script never installs missing dependencies. Install them explicitly so network access and environment mutation are visible.

## Release contents

```text
EDINET-<version>/
├── EDINET.exe
├── .env
├── config/
│   └── database_paths.json
└── data/
    └── databases/
        ├── Base.db
        ├── Standardized.db
        └── Portfolio.db
```

The executable bundles the React production assets, brand assets, ratio definitions, rolling-metric definitions, Python source, and required libraries. Taxonomy archives, logs, job state, saved screens, uploads, exports, tests, docs, and operator data are not bundled.

Never copy development databases or the repository `.env` into a release. The assembly step generates its own files.

## Hidden imports

Router composition is explicit. `EDINET.spec` still lists API modules for auditability and lists orchestrator step packages because step discovery uses `pkgutil`. When adding a step, update the hidden-import list and let the packaged smoke test prove the result.

## CI

The `windows-package-smoke` job runs on the weekly schedule and manual dispatch. It uses the same build script and uploads only `dist/EDINET-*-Release.zip`. Pull requests run the faster unit, integration, frontend, documentation, contract, and static-quality jobs.

## Recovery

- A timeout terminates the command process tree and fails the build; rerun after inspecting the visible stage output.
- If PyInstaller reports a missing module, add the genuinely dynamic import to `EDINET.spec`, then rerun the full smoke build.
- If the SPA check fails, confirm `frontend-v2/dist/index.html` exists after the frontend stage.
- Build output is reproducible from source; delete only the exact generated `build/` and `dist/` directories when cleaning manually.
