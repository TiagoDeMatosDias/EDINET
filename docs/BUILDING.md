# Building EDINET — Distributable EXE

This guide covers how to build a standalone `.exe` from the EDINET source
and package it into a `.zip` that end users can extract and run.

The final `.zip` contains:
- `EDINET.exe` — the compiled application
- `config/database_paths.json` — DB path configuration
- `.env` template — user fills in their EDINET API key
- `data/databases/Base.db` — empty raw-data database
- `data/databases/Standardized.db` — empty standardized database

---

## Prerequisites

- **Python 3.10+** (the same version you develop with)
- **Node.js 18+** with npm (for building the React frontend before packaging)
- **PyInstaller** — `pip install pyinstaller`
- **All runtime dependencies** from `requirements.txt` installed in the
  same environment
- **Windows** (the build produces a Windows `.exe`; cross-compilation is
  not supported by PyInstaller)

---

## Step-by-step

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Build the React frontend

The EXE bundles the production build of the React SPA. Build it before running PyInstaller:

```bash
cd frontend-v2
npm ci
npm run build
cd ..
```

This produces `frontend-v2/dist/` containing `index.html` and hashed JS/CSS chunks under `app-assets/`.

### 3. Build the EXE

```bash
pyinstaller EDINET.spec
```

This runs PyInstaller using the project's spec file, which:
- Bundles all Python source from `src/`, `config.py`, and `main.py`
- Includes the React frontend build (`frontend-v2/dist/`)
- Includes brand assets (`assets/icon.*`, etc.)
- Includes ratio and rolling-metrics definition JSONs
- Explicitly lists dynamically-discovered modules as hidden imports

The output is `dist/EDINET.exe`.

### 4. Prepare the distribution directory

```bash
mkdir dist\EDINET-dist

copy dist\EDINET.exe dist\EDINET-dist\

mkdir dist\EDINET-dist\config
copy config\database_paths.json dist\EDINET-dist\config\

mkdir dist\EDINET-dist\data\databases
```

### 5. Create empty databases in the distribution directory

⚠ **Do not copy the databases from `data/databases/` — they contain your
development data.** Create fresh empty ones in the release directory:

```bash
python -c "
import sqlite3, os
os.makedirs('dist/EDINET-dist/data/databases', exist_ok=True)
for db in ['Base.db', 'Standardized.db']:
    path = f'dist/EDINET-dist/data/databases/{db}'
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE IF NOT EXISTS _placeholder (id INTEGER)')
    conn.commit()
    conn.close()
    print(f'Created {path}')
"
```

### 5. Create the `.env` template

The user must provide their own EDINET API key. Create a template:

```bash
echo # EDINET API key (get yours at https://disclosure.edinet-fsa.go.jp/)> dist\EDINET-dist\.env
echo API_KEY=your_api_key_here>> dist\EDINET-dist\.env
```

### 6. Create the ZIP

```bash
powershell -Command "Compress-Archive -Path dist\EDINET-dist\* -DestinationPath dist\EDINET-Release.zip"
```

---

## What is bundled vs. external

| File / directory                    | Inside EXE? | In ZIP? | Notes |
|-------------------------------------|:-----------:|:-------:|-------|
| Python source (`src/`, `main.py`, `config.py`) | ✅ bundled  | —       | Compiled into the EXE |
| React frontend build (`frontend-v2/dist/`) | ✅ bundled  | —       | Production build served by FastAPI |
| Brand assets (`assets/icon.*`, `ShadeResearch.svg`) | ✅ bundled  | —       | Favicon & branding |
| `ratios_definitions.json`           | ✅ bundled  | —       | Ratio calculation formulas |
| `rolling_metrics.json`              | ✅ bundled  | —       | Rolling-metric column specs |
| `config/database_paths.json`        | —           | ✅      | User can edit DB paths |
| `.env`                              | —           | ✅      | User provides their API key |
| `data/databases/Base.db`            | —           | ✅      | Empty raw-data DB |
| `data/databases/Standardized.db`    | —           | ✅      | Empty standardized DB |
| Taxonomy ZIPs (`assets/taxonomy/`)  | —           | —       | **Not bundled** — downloaded at runtime by the Parse Taxonomy step |

### Why are taxonomy ZIPs excluded?

The `assets/taxonomy/` directory contains ~140 MB of EDINET taxonomy
archives. Including them would:
- Make the EXE >140 MB larger
- Slow down every build
- Go stale as EDINET publishes new taxonomy releases

Instead, the **Parse Taxonomy** orchestrator step downloads taxonomy
archives on demand and caches them in `assets/taxonomy/` relative to
the EXE directory.

### Why are databases external?

The databases are writable at runtime. Embedding them in the EXE would
make them read-only (PyInstaller extracts to a temp directory on every
run, losing all data). The build script creates fresh empty databases
directly in the release directory — it never touches the `data/databases/`
files in your working tree, which contain your development data.

---

## Files not included

These are intentionally excluded from the distribution:

| File / directory                    | Reason |
|-------------------------------------|--------|
| `tests/`                            | Test suite — not needed at runtime |
| `testdata/`                         | Test fixtures |
| `docs/`                             | Developer documentation |
| `logs/`                             | Created at runtime |
| `config/state/`                     | User-specific runtime state |
| `data/raw_documents/`               | Created at runtime by download step |
| `scripts/`                          | Build / dev scripts |
| `.vscode/`, `.idea/`                | IDE configuration |
| `__pycache__/`                      | Bytecode cache |
| `*.pyc`                             | Compiled bytecode |
| `.git/`, `.gitignore`               | Version control |
| `*.db` (other than the two above)   | Dev databases |
| `*.log`                             | Log files |

---

## Troubleshooting

### "No module named X" at runtime

PyInstaller relies on static import analysis. Dynamically-imported modules
(those loaded via `importlib.import_module()` or `pkgutil`) must be listed
in the `hiddenimports` list in `EDINET.spec`.

Common culprits:
- **Orchestrator steps** — discovered via `pkgutil.iter_modules()` in
  `src/orchestrator/common/__init__.py`. Make sure every step package
  under `src/orchestrator/` is listed.
- **sklearn / matplotlib / yfinance** — conditionally or lazily imported.
  Listed in hiddenimports already.
- **uvicorn workers** — if you see uvicorn-related errors, add
  `'uvicorn.loops.auto'`, `'uvicorn.protocols.http.auto'` to hiddenimports.

### "config/database_paths.json not found"

This file must be in `config/` next to `EDINET.exe`. The app looks for it
relative to the EXE's directory. Verify the directory layout:

```
EDINET-dist/
├── EDINET.exe
├── .env
├── config/
│   └── database_paths.json
└── data/
    └── databases/
        ├── Base.db
        └── Standardized.db
```

### "assets/taxonomy/ does not exist"

This directory is created at runtime by the Parse Taxonomy step when it
downloads taxonomy archives. If you need offline taxonomy support, copy
the taxonomy ZIPs manually from a development environment into
`assets/taxonomy/` next to the EXE.

### EXE is very large (>200 MB)

This is expected. PyInstaller bundles Python itself plus all dependencies
(numpy, pandas, scikit-learn, matplotlib, etc.). Size optimizations:
- Use `upx=True` (already enabled in the spec)
- Consider using `--onedir` mode instead of `--onefile` (faster startup,
  larger distribution size but easier updates)
- Exclude unused dependencies from `requirements.txt`

### Console window appears

The EXE is built with `console=True` so the user can see log output and
use Ctrl+C to stop the server. This is intentional — the app is a
server that runs in the background while the user interacts via browser.

---

## Automated build script

A build script that performs all the steps above is available at
`scripts/build.py`. Run it from the project root:

```bash
python scripts/build.py
```

It will:
1. Verify Python version
2. Build the React frontend (`npm run build` in `frontend-v2/`)
3. Install PyInstaller if needed
4. Run `pyinstaller EDINET.spec`
5. Assemble the distribution directory (creates fresh empty databases in
   `dist/EDINET-dist/` — never touches your dev databases)
6. Create the `.zip` at `dist/EDINET-Release.zip`
