# -*- mode: python ; coding: utf-8 -*-

# ── EDINET PyInstaller spec ──────────────────────────────────────────────
# Build the distributable EXE with:
#   pyinstaller EDINET.spec
#
# The resulting dist/EDINET.exe bundles all Python code, the web frontend,
# brand assets, ratio definitions, and rolling-metrics config.
#
# Files that stay OUTSIDE the exe (placed next to it in the .zip):
#   config/database_paths.json   – DB path configuration
#   .env                          – user-provided API key
#   data/databases/Base.db        – empty raw-data database
#   data/databases/Standardized.db – empty standardized database
# ─────────────────────────────────────────────────────────────────────────

# ── Data files bundled inside the exe ────────────────────────────────────
# Each tuple is (source_on_disk, destination_in_bundle).
datas = [
    # ── Web frontend (React SPA, built separately before packaging) ──
    ('frontend-v2/dist', 'frontend-v2/dist'),

    # ── Brand assets (icon, favicon) ──
    ('assets/icon.ico', 'assets'),
    ('assets/icon.png', 'assets'),
    ('assets/icon.svg', 'assets'),
    ('assets/icon_hexagon.svg', 'assets'),
    ('assets/ShadeResearch.svg', 'assets'),

    # ── Ratio & rolling-metrics definitions (loaded relative to __file__) ──
    ('src/orchestrator/generate_ratios/ratios_definitions.json',
     'src/orchestrator/generate_ratios'),
    ('src/orchestrator/generate_rolling_metrics/rolling_metrics.json',
     'src/orchestrator/generate_rolling_metrics'),
]

# Hidden imports for orchestrator discovery and optional libraries.
# Web routers are explicit, but remain listed for packaging auditability.
hiddenimports = [
    # Dynamically discovered orchestrator step packages
    'src.orchestrator.get_documents',
    'src.orchestrator.download_documents',
    'src.orchestrator.populate_company_info',
    'src.orchestrator.import_stock_prices_csv',
    'src.orchestrator.update_stock_prices',
    'src.orchestrator.parse_taxonomy',
    'src.orchestrator.generate_financial_statements',
    'src.orchestrator.generate_ratios',
    'src.orchestrator.generate_rolling_metrics',
    'src.orchestrator.backtest',
    'src.orchestrator.backtest_set',
    'src.orchestrator.update_fx_data',

    # Step service modules (loaded by thin step-wrapper modules)
    'src.orchestrator.generate_financial_statements.service',
    'src.orchestrator.generate_rolling_metrics.service',
    'src.orchestrator.parse_taxonomy.taxonomy_processing',

    # Explicit API composition
    'src.api.router',
    'src.api.pipeline_routes',
    'src.api.job_routes',
    'src.api.system_routes',
    'src.pipeline_jobs',
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
    'src.orchestrator.common',

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
    excludes=[],
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
    name='EDINET',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
