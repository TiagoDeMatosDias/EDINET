# -*- mode: python ; coding: utf-8 -*-

# ── EDINET PyInstaller spec ──────────────────────────────────────────────
# Build the distributable EXE with:
#   pyinstaller EDINET.spec   (or scripts/build.py, which fills release/<platform>/)
#
# The resulting dist/ShadeResearch(.exe) bundles all Python code, the web frontend,
# brand assets, ratio definitions, and rolling-metrics config.
#
# The exe is the whole release. On first start it creates data/ beside itself
# (databases, TLS certificate, logs); settings such as the EDINET API key live
# in data/app.db and are set on the Admin page or with `ShadeResearch.exe config`.
# ─────────────────────────────────────────────────────────────────────────

import sys

# ── Data files bundled inside the exe ────────────────────────────────────
# Each tuple is (source_on_disk, destination_in_bundle).
datas = [
    # ── Web frontend (React SPA, built separately before packaging) ──
    ('frontend-v2/dist', 'frontend-v2/dist'),

    # ── Brand assets (icon, favicon) ──
    ('assets/brand', 'assets/brand'),

    # ── Ratio & rolling-metrics definitions (loaded relative to __file__) ──
    ('src/orchestrator/generate_ratios/ratios_definitions.json',
     'src/orchestrator/generate_ratios'),
    ('src/orchestrator/generate_rolling_metrics/rolling_metrics.json',
     'src/orchestrator/generate_rolling_metrics'),
]

# Hidden imports for orchestrator discovery and optional libraries.
# Pipeline steps are discovered with pkgutil at runtime and never imported by
# name, so collect every orchestrator module rather than listing steps by hand
# (a hand-written list silently dropped steps added later).
# Walk the source tree instead of importing it, so the list does not depend on
# every optional library loading in the build environment.
from pathlib import Path

_orchestrator_root = Path(SPECPATH) / 'src' / 'orchestrator'
orchestrator_modules = sorted(
    '.'.join(path.relative_to(SPECPATH).with_suffix('').parts).removesuffix('.__init__')
    for path in _orchestrator_root.rglob('*.py')
)

hiddenimports = orchestrator_modules + [
    # Explicit API composition
    'src.api.router',
    'src.api.pipeline_routes',
    'src.api.job_routes',
    'src.api.system_routes',
    'src.pipeline_jobs',
    'src.auth',
    'src.auth.api',
    'src.filings',
    'src.filings.api',
    'src.research',
    'src.research.api',
    'src.reports',
    'src.reports.api',
    'src.comparison',
    'src.comparison.api',
    'src.web_app.api.screening',
    'src.web_app.api.security_analysis',
    'src.web_app.api.tags',

    # Backend packages (imported by API routes)
    'src.screening',
    'src.security_analysis',
    'src.backtesting',
    'src.backtesting.api',
    'src.portfolio',
    'src.portfolio.api',

    # Common / utilities
    'src.utilities',

    # Conditionally-imported libraries
    'sklearn',
    'sklearn.linear_model',
    'sklearn.preprocessing',
    'matplotlib',
    'matplotlib.backends.backend_agg',
    'yfinance',
]

# ── Analysis ──────────────────────────────────────────────────────────────
a = Analysis(
    ['main.py'],
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
    name='ShadeResearch',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # UPX-compressed torch DLLs fail to load on Windows (WinError 998 on c10.dll),
    # which takes the translator down with them.
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
