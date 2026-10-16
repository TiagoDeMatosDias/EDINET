# -*- mode: python ; coding: utf-8 -*-

# ── ShadeWorker PyInstaller spec ─────────────────────────────────────────
# Builds the job worker that runs beside the server:
#   pyinstaller ShadeWorker.spec   (scripts/build.py builds this and EDINET.spec)
#
# dist/ShadeWorker(.exe) carries everything a job handler imports, including the
# translation engine (torch CPU + argostranslate) that the server executable
# leaves out. It carries neither the web frontend nor cloudflared.
#
# The worker starts one spawned child process per job slot; in a frozen build
# each child re-runs this executable, which shade_worker.py handles with
# multiprocessing.freeze_support() as its first statement.
# ─────────────────────────────────────────────────────────────────────────

import sys
from pathlib import Path

sys.path.insert(0, SPECPATH)

# ── Data files bundled inside the exe ────────────────────────────────────
datas = [
    # Ratio & rolling-metrics definitions (loaded relative to __file__)
    ('src/orchestrator/generate_ratios/ratios_definitions.json',
     'src/orchestrator/generate_ratios'),
    ('src/orchestrator/generate_rolling_metrics/rolling_metrics.json',
     'src/orchestrator/generate_rolling_metrics'),
]


def _modules_under(*parts):
    """Dotted names of every module below a source folder (walked, not imported)."""
    root = Path(SPECPATH).joinpath(*parts)
    return sorted(
        '.'.join(path.relative_to(SPECPATH).with_suffix('').parts).removesuffix('.__init__')
        for path in root.rglob('*.py')
    )


# Handlers are imported by dotted path at run time (src/jobs/kinds.py), and the
# pipeline discovers its steps with pkgutil, so name them all here.
from src.jobs.kinds import KINDS

handler_modules = sorted({spec.handler.partition(':')[0] for spec in KINDS.values()})

hiddenimports = (
    _modules_under('src', 'orchestrator')
    + _modules_under('src', 'worker')
    + _modules_under('src', 'jobs')
    + handler_modules
    + [
        'multiprocessing',
        'multiprocessing.spawn',
        'multiprocessing.popen_spawn_posix',
        'multiprocessing.popen_spawn_win32',
        'multiprocessing.resource_tracker',
        'multiprocessing.synchronize',
        'src.backtesting',
        'src.screening',
        'src.security_analysis',
        'src.portfolio',
        'src.filings',
        'src.ownership',
        'src.utilities',
        # Translation engine (src/filings/translate.py imports it inside a function)
        'argostranslate',
        'argostranslate.package',
        'argostranslate.translate',
        # Conditionally-imported libraries
        'sklearn',
        'sklearn.linear_model',
        'sklearn.preprocessing',
        'matplotlib',
        'matplotlib.backends.backend_agg',
        'yfinance',
    ]
)

# ── Analysis ──────────────────────────────────────────────────────────────
a = Analysis(
    ['shade_worker.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    # triton is torch's GPU kernel compiler (0.7 GB). torch imports it only when
    # present, and it lingers in an environment that once held a CUDA torch.
    excludes=['triton'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='ShadeWorker',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # UPX-compressed torch DLLs fail to load on Windows (WinError 998 on c10.dll).
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    icon='assets/brand/shade-icon.ico' if sys.platform == 'win32' else None,
    codesign_identity=None,
    entitlements_file=None,
)
