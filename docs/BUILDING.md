# Building the Windows Release

Updated: 2026-07-30

Windows is the packaged target. Use Python 3.12 or 3.13, Node.js 22/npm 10, and the declared `build` dependency group.

```powershell
py -3.13 -m venv .venv3
.\.venv3\Scripts\python.exe -m pip install -e ".[build]" -c constraints.txt
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
5. assembles `dist/ShadeResearch-<version>/` with the executable alone;
6. smoke-tests a copy in an empty temporary folder: it saves `auth.mode=disabled` with `ShadeResearch.exe config set`, starts the executable on a loopback port, checks `/health`, `/`, and that `/api/steps` offers every step package in `src/orchestrator/`, stops it, checks that `data/` beside the copy holds `app.db`, `chat.db`, `market.db`, `filings.db`, and the certificate, then starts it again and checks that the setting survived;
7. writes `dist/ShadeResearch-<version>-Release.zip`.

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
ShadeResearch-<version>/
└── ShadeResearch.exe
```

On first start the executable creates everything else beside itself:

```text
data/
├── app.db        settings (including the EDINET API key), accounts, research, jobs, portfolio
├── chat.db
├── market.db
├── filings.db
├── certs/        self-signed certificate, or a dropped-in one
├── logs/
└── artifacts/    backtests, reports, exports, job uploads
```

Enter the EDINET API key under **Admin → Server settings** after registering the first (administrator) account, or with `ShadeResearch.exe config set edinet.api_key`. Upgrading a folder that holds an older release (`.env`, `config/`, `data/databases/`) is a matter of replacing the executable: on its first start it moves the old layout into `data/` (see [Running the Application](RUNNING.md#moving-from-the-older-layout)).

The executable bundles the React production assets, brand assets, ratio definitions, rolling-metric definitions, Python source, and required libraries. Taxonomy archives, the Argos Japanese-to-English model package, logs, job state, saved screens, uploads, exports, tests, docs, and operator data are not bundled. The translation runtime installs the Argos ja→en model on first use when it is not already available.

Never copy a development `data/` folder into a release.

## Hidden imports

Router composition is explicit. `EDINET.spec` still lists API modules for auditability. Pipeline steps are discovered with `pkgutil` and never imported by name, so the spec collects every `src.orchestrator` module with `collect_submodules`; a new step needs no spec change.

## CI

The `windows-package-smoke` job runs on the weekly schedule and manual dispatch. It uses the same build script and uploads only `dist/ShadeResearch-*-Release.zip`. Pull requests run the faster unit, integration, frontend, documentation, contract, and static-quality jobs.

## Recovery

- A timeout terminates the command process tree and fails the build; rerun after inspecting the visible stage output.
- If PyInstaller reports a missing module, add the genuinely dynamic import to `EDINET.spec`, then rerun the full smoke build.
- If the SPA check fails, confirm `frontend-v2/dist/index.html` exists after the frontend stage.
- Build output is reproducible from source; delete only the exact generated `build/` and `dist/` directories when cleaning manually.
