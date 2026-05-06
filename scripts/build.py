#!/usr/bin/env python3
"""Build script — produces a distributable EDINET-Release.zip.

Prerequisites (run once):
    pip install -r requirements.txt

Usage:
    python scripts/build.py

Output:
    dist/EDINET-Release.zip    — distributable archive
    dist/EDINET-dist/          — staging directory (contents of the zip)

The zip contains:
    EDINET.exe                  — compiled application
    config/database_paths.json  — DB path configuration
    .env                        — template (user fills in API key)
    data/databases/Base.db      — empty raw-data database
    data/databases/Standardized.db — empty standardized database
"""

import os
import shutil
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path

# ── Paths (relative to project root) ─────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = PROJECT_ROOT / "dist"
STAGING_DIR = DIST_DIR / "EDINET-dist"
EXE_SRC = DIST_DIR / "EDINET.exe"
ZIP_DEST = DIST_DIR / "EDINET-Release.zip"
SPEC_FILE = PROJECT_ROOT / "EDINET.spec"

CONFIG_SRC = PROJECT_ROOT / "config" / "database_paths.json"


def step(msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def run(cmd: list[str], **kwargs) -> None:
    """Run a command, printing output live. Raise on failure."""
    print(f"  > {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), **kwargs)
    if result.returncode != 0:
        sys.exit(result.returncode)


# ──────────────────────────────────────────────────────────────────────────
# Step 1: Check Python
# ──────────────────────────────────────────────────────────────────────────
step("1/5  Checking Python version")
py_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
print(f"  Python {py_ver}")
if sys.version_info < (3, 10):
    print("  ERROR: Python 3.10+ is required.")
    sys.exit(1)

# ──────────────────────────────────────────────────────────────────────────
# Step 2: Ensure PyInstaller is installed
# ──────────────────────────────────────────────────────────────────────────
step("2/5  Checking PyInstaller")
try:
    import PyInstaller  # noqa: F401
    print("  PyInstaller found.")
except ImportError:
    print("  Installing PyInstaller...")
    run([sys.executable, "-m", "pip", "install", "pyinstaller"])

# ──────────────────────────────────────────────────────────────────────────
# Step 3: Clean and build the EXE
# ──────────────────────────────────────────────────────────────────────────
step("3/5  Building EDINET.exe (this may take several minutes)")

# Clean previous build artifacts
for d in ["build", "dist"]:
    dir_path = PROJECT_ROOT / d
    if dir_path.exists():
        shutil.rmtree(dir_path)
        print(f"  Removed {dir_path}")

run(["pyinstaller", str(SPEC_FILE)])

if not EXE_SRC.exists():
    print(f"  ERROR: {EXE_SRC} was not created. Check PyInstaller output above.")
    sys.exit(1)
print(f"  Created {EXE_SRC}")

# ──────────────────────────────────────────────────────────────────────────
# Step 4: Assemble distribution directory
# ──────────────────────────────────────────────────────────────────────────
step("4/5  Assembling distribution directory")

if STAGING_DIR.exists():
    shutil.rmtree(STAGING_DIR)
STAGING_DIR.mkdir(parents=True, exist_ok=True)

# Copy EXE
shutil.copy2(EXE_SRC, STAGING_DIR / "EDINET.exe")
print(f"  Copied EDINET.exe")

# Copy config
cfg_dest = STAGING_DIR / "config"
cfg_dest.mkdir(parents=True, exist_ok=True)
shutil.copy2(CONFIG_SRC, cfg_dest / "database_paths.json")
print(f"  Copied config/database_paths.json")

# Create empty databases directly in the release directory
# (never touch the dev databases in data/databases/ — they contain real data)
db_dest = STAGING_DIR / "data" / "databases"
db_dest.mkdir(parents=True, exist_ok=True)
for db_name in ["Base.db", "Standardized.db"]:
    conn = sqlite3.connect(str(db_dest / db_name))
    conn.execute("CREATE TABLE IF NOT EXISTS _placeholder (id INTEGER)")
    conn.commit()
    conn.close()
print(f"  Created empty databases in data/databases/")

# Create .env template
env_path = STAGING_DIR / ".env"
env_path.write_text(
    "# EDINET API key — get yours at https://disclosure.edinet-fsa.go.jp/\n"
    "API_KEY=your_api_key_here\n",
    encoding="utf-8",
)
print(f"  Created .env template")

# ──────────────────────────────────────────────────────────────────────────
# Step 5: Create ZIP
# ──────────────────────────────────────────────────────────────────────────
step("5/5  Creating ZIP archive")

if ZIP_DEST.exists():
    ZIP_DEST.unlink()

with zipfile.ZipFile(str(ZIP_DEST), "w", zipfile.ZIP_DEFLATED) as zf:
    for file_path in sorted(STAGING_DIR.rglob("*")):
        if file_path.is_file():
            arcname = file_path.relative_to(STAGING_DIR)
            zf.write(file_path, arcname)

zip_size_mb = ZIP_DEST.stat().st_size / (1024 * 1024)
print(f"  Created {ZIP_DEST} ({zip_size_mb:.1f} MB)")

# ──────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("  Build complete!")
print(f"  {ZIP_DEST}")
print(f"\n  Contents:")
for file_path in sorted(STAGING_DIR.rglob("*")):
    if file_path.is_file():
        print(f"    {file_path.relative_to(STAGING_DIR)}")
print(f"{'='*60}")
