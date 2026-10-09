"""One-time move from the old storage layout into the data folder.

The old layout kept nine databases in ``data/databases`` and
``config/state/databases`` (relocatable through
``config/database_paths.json``), the EDINET API key in ``.env``, the chat key
ring in ``config/state/secrets``, and generated files under ``config/state``,
``data/``, and ``logs/``. The new layout is described in ``src.paths``:

    app.db      auth.db + research.db + pipeline_jobs.db + Portfolio.db,
                plus the API key and the chat key ring as settings, and
                each saved backtest's owner.json and meta.json as a row
    chat.db     chat.db
    market.db   Standardized.db + Base.db + Bonds.db
    filings.db  Filings.db

``migrate_legacy_layout`` runs before the server starts. Nothing is deleted:
databases merged into another file and imported files are renamed with a
``.migrated`` suffix; whole databases and folders are moved, which is a rename
on the same disk. Every part is skipped once its target exists, so an
interrupted run resumes where it stopped.

Only the default data folder is migrated. With ``EDINET_DATA_DIR`` set (tests,
scratch runs) the legacy files beside the application are never touched.

Run with the server and pipeline stopped::

    python main.py migrate [--dry-run]
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src import paths
from src.orchestrator.common.sqlite import quote_identifier

logger = logging.getLogger(__name__)

MIGRATED_SUFFIX = ".migrated"
_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")

# Old configuration key -> file name.
_LEGACY_DATABASES = {
    "db1": "Base.db",
    "db2": "Standardized.db",
    "filings_db": "Filings.db",
    "bonds_db": "Bonds.db",
    "db3": "Portfolio.db",
    "auth_db": "auth.db",
    "research_db": "research.db",
    "pipeline_jobs_db": "pipeline_jobs.db",
    "chat_db": "chat.db",
}
_REBUILDABLE = frozenset({"db1", "db2", "filings_db", "bonds_db"})
# Databases merged into app.db, with the component that owns any unscoped
# schema_migrations rows they carry.
_APP_DB_SOURCES = {
    "auth_db": "auth",
    "research_db": "research",
    "pipeline_jobs_db": "pipeline_jobs",
    "db3": "portfolio",
}
# Folders of generated files: old location relative to the application
# folder -> new location relative to the data folder.
_FOLDERS = {
    Path("config/state/jobs"): Path("artifacts/jobs"),
    Path("config/state/manual_jobs"): Path("artifacts/manual_jobs"),
    Path("config/state/exports"): Path("artifacts/exports"),
    Path("data/Backtests"): Path("artifacts/backtests"),
    Path("data/reports"): Path("artifacts/reports"),
    Path("data/raw_documents"): Path("artifacts/raw_documents"),
    Path("data/certs"): Path("certs"),
    Path("logs"): Path("logs"),
}
# .env names whose value becomes the EDINET API key setting.
_API_KEY_NAMES = ("EDINET_API_TOKEN", "API_KEY")
_PLACEHOLDER_API_KEYS = frozenset({"", "your_api_key_here"})
API_KEY_SETTING = "edinet.api_key"
CHAT_KEY_RING_SETTING = "chat.message_keys"


class LegacyLayoutError(RuntimeError):
    """The old layout cannot be migrated as it stands; the message says why."""


@dataclass(frozen=True)
class Step:
    description: str
    run: Callable[[], None]


def _resolve(app_dir: Path, value: str) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else app_dir / candidate


def legacy_database_paths(app_dir: Path) -> dict[str, Path]:
    """Where each old database lives, whether or not the file exists."""
    config_file = app_dir / "config" / "database_paths.json"
    configured: dict[str, str] = {}
    if config_file.is_file():
        configured = json.loads(config_file.read_text(encoding="utf-8"))
    found: dict[str, Path] = {}
    for key, filename in _LEGACY_DATABASES.items():
        if configured.get(key):
            found[key] = _resolve(app_dir, configured[key])
        elif key in _REBUILDABLE:
            found[key] = app_dir / "data" / "databases" / filename
        else:
            path = app_dir / "config" / "state" / "databases" / filename
            older = app_dir / "data" / "databases" / filename
            found[key] = older if not path.exists() and older.exists() else path
    return found


def _uses_default_data_dir() -> bool:
    return not os.getenv("EDINET_DATA_DIR", "").strip()


# -- file helpers -------------------------------------------------------------


def _move(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))
    for suffix in _SIDECAR_SUFFIXES:
        sidecar = Path(f"{source}{suffix}")
        if sidecar.exists():
            shutil.move(str(sidecar), f"{target}{suffix}")


def _retire(path: Path) -> None:
    """Keep a merged or imported file beside its old name, out of the way."""
    if path.exists():
        _move(path, Path(f"{path}{MIGRATED_SUFFIX}"))


def _ensure_not_in_use(path: Path) -> None:
    """Fold the write-ahead log into the file; fails if anything has it open.

    Leaving WAL mode needs the only connection to the database, and an
    exclusive lock covers databases not in WAL mode. The application turns
    WAL back on when it next opens the file.
    """
    conn = sqlite3.connect(path, timeout=0, isolation_level=None)
    try:
        (mode,) = conn.execute("PRAGMA journal_mode = DELETE").fetchone()
        if str(mode).lower() != "delete":
            raise sqlite3.OperationalError("another connection has it open")
        conn.execute("BEGIN EXCLUSIVE")
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError as exc:
        raise LegacyLayoutError(
            f"{path} is in use ({exc}); stop the server and the pipeline, then start again."
        ) from exc
    finally:
        conn.close()


def _read_env_file(path: Path) -> dict[str, str]:
    """Parse ``KEY=VALUE`` lines the way the old ``.env`` loader did."""
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


# -- copying databases --------------------------------------------------------


def _copy_database(conn: sqlite3.Connection, source: Path, component: str | None) -> None:
    """Copy every table, index, trigger, and view of ``source`` into ``main``.

    A table that already exists with the same definition is taken to be
    copied by an earlier, interrupted run. Unscoped ``schema_migrations`` rows
    are recorded under ``component``.
    """
    conn.execute("ATTACH DATABASE ? AS src", (str(source),))
    try:
        objects = conn.execute(
            "SELECT type, name, sql FROM src.sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
            "ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, rowid"
        ).fetchall()
        skipped: set[str] = set()
        for kind, name, sql in objects:
            if name == "schema_migrations":
                _copy_migrations(conn, component)
                continue
            if str(sql).upper().startswith("CREATE VIRTUAL"):
                raise LegacyLayoutError(f"{source} holds virtual table {name}, which cannot be merged")
            existing = conn.execute(
                "SELECT sql FROM main.sqlite_master WHERE name = ?", (name,)
            ).fetchone()
            if existing is not None:
                if existing[0] != sql:
                    raise LegacyLayoutError(
                        f"{source} and the merged database both define {kind} {name} differently"
                    )
                skipped.add(name)
                continue
            conn.execute(sql)
            if kind == "table":
                conn.execute(
                    f"INSERT INTO main.{quote_identifier(name)} SELECT * FROM src.{quote_identifier(name)}"
                )
        _copy_sequences(conn, skipped)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.execute("DETACH DATABASE src")


def _copy_migrations(conn: sqlite3.Connection, component: str | None) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS main.schema_migrations ("
        "component TEXT NOT NULL, version INTEGER NOT NULL, applied_at TEXT NOT NULL, "
        "PRIMARY KEY (component, version))"
    )
    columns = {row[1] for row in conn.execute("PRAGMA src.table_info(schema_migrations)")}
    if "component" in columns:
        conn.execute(
            "INSERT OR IGNORE INTO main.schema_migrations "
            "SELECT component, version, applied_at FROM src.schema_migrations"
        )
    elif component is not None:
        conn.execute(
            "INSERT OR IGNORE INTO main.schema_migrations "
            "SELECT ?, version, applied_at FROM src.schema_migrations",
            (component,),
        )


def _copy_sequences(conn: sqlite3.Connection, skipped: set[str]) -> None:
    """Carry AUTOINCREMENT counters over, so deleted ids are never reused."""
    has_source = conn.execute(
        "SELECT 1 FROM src.sqlite_master WHERE name = 'sqlite_sequence'"
    ).fetchone()
    if not has_source:
        return
    for name, seq in conn.execute("SELECT name, seq FROM src.sqlite_sequence").fetchall():
        if name in skipped:
            continue
        updated = conn.execute(
            "UPDATE main.sqlite_sequence SET seq = MAX(seq, ?) WHERE name = ?", (seq, name)
        ).rowcount
        if not updated:
            conn.execute("INSERT INTO main.sqlite_sequence(name, seq) VALUES (?, ?)", (name, seq))


def _merge_into(target: Path, sources: list[tuple[Path, str | None]]) -> None:
    conn = sqlite3.connect(target, isolation_level=None)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        for source, component in sources:
            conn.execute("BEGIN")
            _copy_database(conn, source, component)
    finally:
        conn.close()


# -- the plan -----------------------------------------------------------------


def _app_db_steps(legacy: dict[str, Path], app_db: Path) -> list[Step]:
    sources = [(legacy[key], component) for key, component in _APP_DB_SOURCES.items() if legacy[key].is_file()]
    if app_db.exists() or not sources:
        return []

    def build() -> None:
        partial = Path(f"{app_db}.partial")
        if partial.exists():
            partial.unlink()
        partial.parent.mkdir(parents=True, exist_ok=True)
        _merge_into(partial, sources)
        os.replace(partial, app_db)
        for source, _component in sources:
            _retire(source)

    names = ", ".join(source.name for source, _ in sources)
    return [Step(f"Merge {names} into {app_db}", build)]


def _setting_steps(app_dir: Path, app_db: Path) -> list[Step]:
    from src.settings.store import SettingsStore

    steps: list[Step] = []
    key_ring = app_dir / "config" / "state" / "secrets" / "chat_message_keys.json"
    if key_ring.is_file():
        def import_key_ring() -> None:
            ring = json.loads(key_ring.read_text(encoding="utf-8"))
            stored = SettingsStore(app_db).set_if_missing(CHAT_KEY_RING_SETTING, ring)
            if stored != ring:
                raise LegacyLayoutError(
                    f"{app_db} already holds a different chat key ring than {key_ring}; "
                    "move one of them aside and start again."
                )
            _retire(key_ring)

        steps.append(Step(f"Store the chat key ring from {key_ring} in {app_db}", import_key_ring))

    env_file = app_dir / ".env"
    if env_file.is_file():
        def import_env() -> None:
            values = _read_env_file(env_file)
            api_key = next(
                (values[name] for name in _API_KEY_NAMES if values.get(name, "") not in _PLACEHOLDER_API_KEYS),
                "",
            )
            if api_key:
                SettingsStore(app_db).set_if_missing(API_KEY_SETTING, api_key)
            _retire(env_file)

        steps.append(Step(f"Store the EDINET API key from {env_file} in {app_db}", import_env))
    return steps


def _market_db_steps(legacy: dict[str, Path], market_db: Path) -> list[Step]:
    standardized, base, bonds = legacy["db2"], legacy["db1"], legacy["bonds_db"]
    extra = [path for path in (base, bonds) if path.is_file()]
    if market_db.exists() or not (standardized.is_file() or extra):
        return []

    def build() -> None:
        target = standardized if standardized.is_file() else Path(f"{market_db}.partial")
        if not standardized.is_file() and target.exists():
            target.unlink()
        for source in extra:
            _merge_into(target, [(source, None)])
            _retire(source)
        _move(target, market_db)

    names = ", ".join(path.name for path in (standardized, *extra) if path.is_file())
    return [Step(f"Combine {names} into {market_db}", build)]


def _backtest_index_steps(app_dir: Path, data_dir: Path, app_db: Path) -> list[Step]:
    """Record each saved backtest's owner.json and meta.json as a row in app.db."""
    folder = data_dir / "artifacts" / "backtests"
    legacy_folder = app_dir / "data" / "Backtests"
    files = ("owner.json", "meta.json")
    if not legacy_folder.is_dir() and not any(folder.glob("*/owner.json")) and not any(folder.glob("*/meta.json")):
        return []

    def index() -> None:
        from src.backtesting.catalog import BacktestCatalog

        catalog = BacktestCatalog(app_db)
        for result in sorted(path for path in folder.iterdir() if path.is_dir()) if folder.is_dir() else []:
            owner, meta = (_read_json(result / name) for name in files)
            if owner is not None:
                catalog.record_owner(result.name, owner.get("owner_user_id") or None)
            if meta is not None:
                catalog.describe(
                    result.name,
                    kind=str(meta.get("kind") or ""),
                    title=str(meta.get("title") or ""),
                    subtitle=str(meta.get("subtitle") or ""),
                    headline=meta.get("headline") if isinstance(meta.get("headline"), dict) else {},
                )
            for name in files:
                _retire(result / name)

    return [Step(f"Record the owner and description of each saved backtest in {app_db}", index)]


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _move_step(source: Path, target: Path) -> list[Step]:
    if not source.exists() or target.exists() or source.resolve() == target.resolve():
        return []
    return [Step(f"Move {source} to {target}", lambda: _move(source, target))]


def _retire_step(path: Path, reason: str) -> list[Step]:
    if not path.exists():
        return []
    return [Step(f"Rename {path} to {path.name}{MIGRATED_SUFFIX} ({reason})", lambda: _retire(path))]


def plan(app_dir: Path | None = None, data_dir: Path | None = None) -> list[Step]:
    """Every step still needed to reach the new layout, in order."""
    app_dir = app_dir or paths.app_dir()
    data_dir = data_dir or paths.data_dir()
    legacy = legacy_database_paths(app_dir)
    app_db = data_dir / "app.db"
    steps = [
        *_app_db_steps(legacy, app_db),
        *_setting_steps(app_dir, app_db),
        *_move_step(legacy["chat_db"], data_dir / "chat.db"),
        *_market_db_steps(legacy, data_dir / "market.db"),
        *_move_step(legacy["filings_db"], data_dir / "filings.db"),
    ]
    for old, new in _FOLDERS.items():
        steps += _move_step(app_dir / old, data_dir / new)
    steps += _backtest_index_steps(app_dir, data_dir, app_db)
    steps += _retire_step(
        app_dir / "config" / "state" / "screening_history.jsonl",
        "its entries have no owner; history is now kept per user",
    )
    steps += _retire_step(app_dir / "config" / "database_paths.json", "database locations are settings now")
    return steps


def databases_in_use_check(app_dir: Path | None = None) -> list[Path]:
    """The old database files a migration would read or move."""
    return [path for path in legacy_database_paths(app_dir or paths.app_dir()).values() if path.is_file()]


def pending() -> bool:
    """Whether the default data folder still has to be migrated."""
    return _uses_default_data_dir() and bool(plan())


def pending_databases() -> list[Path]:
    """Old database files whose new home does not exist yet (cheap: only stats)."""
    if not _uses_default_data_dir():
        return []
    data_dir = paths.data_dir()
    targets = {
        "db1": "market.db", "db2": "market.db", "bonds_db": "market.db", "filings_db": "filings.db",
        "chat_db": "chat.db", "db3": "app.db", "auth_db": "app.db", "research_db": "app.db",
        "pipeline_jobs_db": "app.db",
    }
    legacy = legacy_database_paths(paths.app_dir())
    return [
        path for key, path in legacy.items()
        if path.is_file() and not (data_dir / targets[key]).exists()
    ]


def _remove_empty_folders(app_dir: Path) -> None:
    for folder in (
        app_dir / "config" / "state" / "databases",
        app_dir / "config" / "state" / "secrets",
        app_dir / "config" / "state",
        app_dir / "config",
        app_dir / "data" / "databases",
    ):
        try:
            folder.rmdir()
        except OSError:
            pass


def migrate_legacy_layout(*, dry_run: bool = False, report: Callable[[str], None] = print) -> list[str]:
    """Move the old layout into the data folder; return what was (or would be) done."""
    if not _uses_default_data_dir():
        return []
    app_dir = paths.app_dir()
    steps = plan(app_dir, paths.data_dir())
    if not steps:
        return []
    if not dry_run:
        for database in databases_in_use_check(app_dir):
            _ensure_not_in_use(database)
    report(("Would migrate" if dry_run else "Migrating") + " to the data folder layout:")
    done = []
    for step in steps:
        report(f"  {step.description}")
        if not dry_run:
            step.run()
        done.append(step.description)
    if not dry_run:
        _remove_empty_folders(app_dir)
        from src.orchestrator.common import db_config

        db_config.reload()
    return done
