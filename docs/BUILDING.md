# Building the Release

Updated: 2026-10-16

Windows and Linux are the packaged targets. PyInstaller cannot cross-compile, so each is built on its own platform (the `release` workflow builds both).

## One step

Double-click `build.sh` on Linux or `build.bat` on Windows, in the project folder. It needs Python 3.12 or 3.13 and Node.js 22/npm 10 installed, and fills `release/linux/` or `release/windows/` with the two executables: `ShadeResearch`, the server, and `ShadeWorker`, the job worker.

The first run creates a build environment in `.venv-build/` (the CPU build of torch, then the `build` dependency group), which takes a few minutes; it is reinstalled only when `pyproject.toml` or `constraints.txt` changes. The launcher then runs `scripts/build.py`, described below, and keeps its window open until Enter is pressed. On Linux the file manager must be set to run executable files rather than open them; the launcher opens a terminal window itself. Arguments are passed on: `./build.sh --check` runs the preflight only.

## By hand

Use Python 3.12 or 3.13, Node.js 22/npm 10, and the declared `build` dependency group.

```powershell
py -3.13 -m venv .venv3
.\.venv3\Scripts\python.exe -m pip install torch --index-url https://download.pytorch.org/whl/cpu
.\.venv3\Scripts\python.exe -m pip install -e ".[build]" -c constraints.txt
```

Install the CPU build of torch first, on both platforms. Only `ShadeWorker` carries it: the translator runs in the worker and reaches torch through `argostranslate` → `stanza` and runs on the CPU only, but on Linux the default wheel is the CUDA build, whose NVIDIA libraries cannot be left out of the bundle because torch links against them. See [Size](#size).

## Canonical workflow

`EDINET.spec` (the server, `ShadeResearch`) and `ShadeWorker.spec` (the job worker) are the versioned canonical PyInstaller specifications. `scripts/build.py` is the only supported orchestration command:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py
```

The script, in order:

1. verifies supported Python, Node/npm, both spec files, the frontend lockfile, PyInstaller, and that torch is a CPU build;
2. runs `npm ci` and the production frontend build with hard timeouts;
3. removes the repository's `build/` and `dist/` directories left by a failed earlier run;
4. runs PyInstaller through the active interpreter for each spec, with a 600-second default cap each. `ShadeWorker.spec` bundles the job handlers, the whole orchestrator, torch, and the translator, but no frontend and no `cloudflared`; `EDINET.spec` leaves the translation engine out (`torch`, `argostranslate`, `ctranslate2`, `sentencepiece`, `stanza`, `spacy`) and first downloads Cloudflare's current `cloudflared` release for this platform (about 40 MB, from `github.com/cloudflare/cloudflared`) into `tools/bin/` and bundles it; when the download fails it bundles the copy already there, with a warning, and fails only when there is none;
5. replaces `release/<platform>/` (`windows` or `linux`, whichever this host is) with the two executables alone; the other platform's folder is left untouched;
6. smoke-tests a copy of both executables in an empty temporary folder: it saves `auth.mode=disabled` with `ShadeResearch.exe config set`, starts the server on a loopback port, checks `/health`, `/`, that `/api/steps` offers every step package in `src/orchestrator/`, and that the application finds the `cloudflared` it carries (`/api/admin/server/tunnel`); waits up to a minute for the local worker the server starts to come online with data access (`/api/admin/workers`); runs a diagnostic job through it (`/api/admin/workers/echo`) and translates a Japanese sentence (`/api/filings/translate`, up to `--translation-timeout` seconds, 120 by default, because the first translation loads the models and installs the Argos ja→en package when it is missing, which needs the network); stops the server and checks that no `ShadeWorker` process outlives it; checks that `data/` beside the copy holds `app.db`, `chat.db`, `jobs.db`, `market.db`, `filings.db`, and the certificate; then starts the server again and checks that the setting survived;
7. removes `build/` and `dist/`, PyInstaller's work and output folders, so `release/` is all a successful build leaves. A failed build keeps them: `build/EDINET/warn-EDINET.txt` and `xref-EDINET.html` (and the same under `build/ShadeWorker/`) show what PyInstaller did and did not find.

Run non-mutating preflight only:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py --check
```

Override bounded stages when the build host is unusually slow:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py --command-timeout 180 --smoke-timeout 45
```

Smoke-test executables that are already built, without building (the folder must hold both):

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py --smoke-only release\windows
```

The build script never installs missing dependencies. Install them explicitly so network access and environment mutation are visible. Its own network use is `npm ci`, the `cloudflared` download, and, in the smoke test, the Argos model download when the build machine has none.

## Release contents

```text
release/
├── windows/
│   ├── ShadeResearch.exe
│   └── ShadeWorker.exe
└── linux/
    ├── ShadeResearch
    └── ShadeWorker
```

Keep the two executables in the same folder. Double-click `ShadeResearch`: it creates or reuses `data/` in the folder it is started from, starts the server and the job worker beside it, and opens the workstation in the default browser (set `EDINET_NO_BROWSER=1` to skip that). Without `ShadeWorker` beside it the server still runs, but **Admin → Workers** reports the worker missing and pipelines, backtests, screens, and translations wait for a worker. On Linux the file manager must be set to run executable files rather than open them in an editor.

On first start the executable creates everything else beside itself:

```text
data/
├── app.db        settings (including the EDINET API key), accounts, research, workers, portfolio
├── chat.db
├── jobs.db       the job queue
├── market.db
├── filings.db
├── certs/        self-signed certificate, or a dropped-in one
├── logs/         server.log, worker-local.log
└── artifacts/    backtests, reports, exports, job workspaces
```

Enter the EDINET API key under **Admin → Server settings** after registering the first (administrator) account, or with `ShadeResearch.exe config set edinet.api_key`. Upgrading is a matter of replacing the executables (both of them). A folder that holds an older release (`.env`, `config/`, `data/databases/`) is moved too: on its first start it moves the old layout into `data/` (see [Running the Application](RUNNING.md#moving-from-the-older-layout)).

`ShadeResearch` bundles the React production assets, brand assets, ratio definitions, rolling-metric definitions, Python source, required libraries, and `cloudflared` (Cloudflare's tunnel client, Apache-2.0 licensed), which **Admin → Remote access** runs from inside the bundle. `ShadeWorker` bundles the Python source, the ratio and rolling-metric definitions, the data and analysis libraries, and the translator (torch, Argos Translate, CTranslate2, Stanza). Taxonomy archives, the Argos Japanese-to-English model package, logs, job state, saved screens, uploads, exports, tests, docs, and operator data are not bundled. The translation runtime installs the Argos ja→en model on first use when it is not already available.

Never copy a development `data/` folder into a release.

## Size

On Linux the server is about 160 MB and the worker about 440 MB, on Windows about 150 MB and 300 MB; together they replace the single 485 MB executable of earlier releases (345 MB on Windows), because the translator is only in the worker and the frontend and `cloudflared` only in the server. The Windows libraries are smaller before compression (torch alone is 376 MB against 668 MB). UPX is switched off in both specs: it saved about 65 MB on Windows, but torch DLLs compressed with it fail to load (`WinError 998` on `c10.dll`), which breaks the translator. Before compression the Linux parts are:

| Part | Size | In |
| --- | --- | --- |
| Translator: torch 670 MB, spaCy 120 MB, CTranslate2 140 MB, the rest 90 MB | 1,020 MB | worker |
| numpy, scipy, pandas, scikit-learn, matplotlib | 205 MB | both |
| Python, the application, FastAPI, cryptography and other libraries | 130 MB | both |
| Web frontend | 33 MB | server |
| `cloudflared` (53 MB on Windows) | 38 MB | server |

A CUDA build of torch turns that 1.4 GB into 5 GB (a 2.8 GB executable): 2.5 GB of NVIDIA libraries, 0.4 GB of CUDA code inside torch, and 0.7 GB of `triton`. `scripts/build.py` therefore refuses to package a CUDA torch, and both specs exclude `triton`, which stays installed after swapping the torch build in an existing environment.

## Building the Windows executable from Linux

With no Windows machine, `release/windows/ShadeResearch.exe` was built inside Wine 11 (the `tobix/pywine:3.13` container, upgraded with `winehq-devel`; Wine 10 lacks a C runtime function numpy needs). Build `ShadeWorker.spec` the same way. Three Wine-only steps: PyInstaller's binary-dependency scan imports every `torch.*` submodule and Wine crashes on some, so that loop skips them (`torch` itself is still imported); the spec enumerates `src.orchestrator` from the source tree instead of importing it; and `pip install msvc-runtime` puts Microsoft's C++ runtime DLLs beside `python.exe`, because PyInstaller leaves out Wine's own copies and torch would then need the Visual C++ Redistributable on the target PC. The spec's `cloudflared.exe` download works there unchanged. The result was smoke-tested under Wine (`--help`, `config set`, `/health`, `/api/steps`, the bundled `cloudflared`, `data/` created, one translation). Prefer the `release` workflow on a real Windows runner for published releases.

## Hidden imports

Router composition is explicit. `EDINET.spec` still lists API modules for auditability. Pipeline steps are discovered with `pkgutil` and never imported by name, so both specs list every `src.orchestrator` module by walking the source tree; a new step needs no spec change. Job handlers are named by dotted path in `src/jobs/kinds.py`, and `ShadeWorker.spec` adds every one of them, with every `src.worker` and `src.jobs` module, so a new job kind needs no spec change either; `tests/unit/test_packaging_specs.py` checks that each handler module exists and that the server spec leaves the translator out.

## CI

The `release` workflow builds both platforms on their own runners and runs on the weekly schedule and manual dispatch. It installs the CPU build of torch, uses the same build script, and uploads only `release/`: both executables per platform. Pull requests run the faster unit, integration, frontend, documentation, contract, and static-quality jobs.

## Recovery

- A timeout terminates the command process tree and fails the build; rerun after inspecting the visible stage output.
- If PyInstaller reports a missing module, add the genuinely dynamic import to `EDINET.spec` (server) or `ShadeWorker.spec` (worker), then rerun the full smoke build.
- If the smoke test's worker never comes online, the error shows the server's last **Admin → Workers** answer (launcher state, restarts, and the worker's last exit code). Start the release's `ShadeResearch` by hand and read `data/logs/worker-local.log` beside it.
- If the SPA check fails, confirm `frontend-v2/dist/index.html` exists after the frontend stage.
- Build output is reproducible from source; `build/` and `dist/` left by a failed build can be deleted by hand.
