"""Filing storage: the index, the yearly files, the write order, checks and the server guard."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from src.filings import maintenance
from src.filings.catalog import FilingCatalog
from src.filings.storage import (
    FilingStorage,
    FilingStorageError,
    MissingSubmissionDateError,
    ShardReader,
    ShardUnavailableError,
    shard_for,
)
from src.orchestrator.common.sqlite import (
    clear_protected_database_paths,
    connect_read,
    connect_write,
    protect_database_paths,
)
from tests.filing_fixtures import filing_row, report_zip

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def catalog(tmp_path) -> FilingCatalog:
    catalog = FilingCatalog(tmp_path / "data" / "filings.db")
    catalog.upsert_filing(filing_row("S100A", "E00001", "2019-06-20T09:00"))
    catalog.upsert_filing(filing_row("S100B", "E00001", "2020-06-19 15:00"))
    catalog.upsert_filing(filing_row("S100C", "E00002", "2020-07-01T09:00"))
    return catalog


def _scalar(path: Path, sql: str, params: tuple = ()):
    conn = connect_read(path)
    try:
        row = conn.execute(sql, params).fetchone()
        return None if row is None else row[0]
    finally:
        conn.close()


# --- choosing the shard ------------------------------------------------------


@pytest.mark.parametrize(
    ("submitted_at", "shard"),
    [("2026-07-27 17:07", "2026"), ("2019-06-20T09:00:00+09:00", "2019"), ("2021", "2021"), (" 2016-01-04 ", "2016")],
)
def test_the_shard_is_the_submission_year(submitted_at, shard):
    assert shard_for(submitted_at) == shard


@pytest.mark.parametrize("submitted_at", [None, "", "   ", "20190620", "unknown", "0000-01-01"])
def test_a_filing_without_a_submission_year_is_refused(submitted_at):
    with pytest.raises(MissingSubmissionDateError, match="no submission date"):
        shard_for(submitted_at)


def test_a_new_install_creates_the_index_and_yearly_files_as_filings_arrive(catalog):
    assert catalog.layout == "split-v1"
    assert sorted(path.name for path in (catalog.path.parent / "filings").iterdir() if path.suffix == ".db") == [
        "filings_2019.db",
        "filings_2020.db",
    ]
    assert {row["shard"]: row["filing_count"] for row in catalog.shards()} == {"2019": 1, "2020": 2}
    # The index lists every filing with its shard and holds no archive.
    assert _scalar(catalog.path, "SELECT shard FROM filings WHERE doc_id = 'S100B'") == "2020"
    assert _scalar(catalog.path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'filing_archives'") == 0
    assert catalog.get_archive_content("S100B") == report_zip("S100B")


def test_a_filing_keeps_its_shard_when_its_submission_date_changes(catalog):
    catalog.upsert_filing(filing_row("S100A", "E00001", "2021-01-05T09:00", content=report_zip("S100A", "v2")))

    assert _scalar(catalog.path, "SELECT shard FROM filings WHERE doc_id = 'S100A'") == "2019"
    assert _scalar(catalog.path, "SELECT submitted_at FROM filings WHERE doc_id = 'S100A'") == "2021-01-05T09:00"
    assert catalog.get_archive_content("S100A") == report_zip("S100A", "v2")
    assert not (catalog.path.parent / "filings" / "filings_2021.db").exists()


def test_a_write_for_an_unknown_filing_needs_its_date(catalog):
    with pytest.raises(FilingStorageError, match="not in the filing catalog"):
        catalog.set_status("S100NEW", "parsed", None, "2026-01-01")
    with pytest.raises(MissingSubmissionDateError):
        catalog.upsert_filing(filing_row("S100NEW", submitted_at=""))


# --- write order and repair ----------------------------------------------------


def test_a_crash_after_the_shard_commit_is_repaired_by_check(catalog, monkeypatch):
    def crash(*_args, **_kwargs):
        raise RuntimeError("power cut")

    monkeypatch.setattr(catalog.storage, "publish", crash)
    with pytest.raises(RuntimeError):
        catalog.upsert_filing(filing_row("S100D", "E00003", "2020-08-01T09:00"))
    monkeypatch.undo()

    # The yearly file has it; the index does not list it yet.
    assert _scalar(catalog.storage.shard_path("2020"), "SELECT COUNT(*) FROM filings WHERE doc_id = 'S100D'") == 1
    assert catalog.get_filing("S100D") is None

    report = maintenance.check(catalog.storage)
    assert not report["ok"]
    assert [item["unlisted"] for item in report["shards"] if item["shard"] == "2020"] == [["S100D"]]

    repaired = maintenance.check(catalog.storage, repair=True)
    assert repaired["repaired"]
    assert _scalar(catalog.path, "SELECT shard FROM filings WHERE doc_id = 'S100D'") == "2020"
    assert {row["shard"]: row["filing_count"] for row in catalog.shards()}["2020"] == 3
    assert maintenance.check(catalog.storage)["ok"]

    # Re-ingesting the same filing is idempotent.
    catalog.upsert_filing(filing_row("S100D", "E00003", "2020-08-01T09:00"))
    assert {row["shard"]: row["filing_count"] for row in catalog.shards()}["2020"] == 3


def test_a_filing_lost_from_its_file_is_marked_missing_never_deleted(catalog):
    conn = connect_write(catalog.storage.shard_path("2020"))
    try:
        conn.execute("DELETE FROM filing_archives WHERE doc_id = 'S100C'")
        conn.execute("DELETE FROM filings WHERE doc_id = 'S100C'")
        conn.commit()
    finally:
        conn.close()

    report = maintenance.check(catalog.storage, shard="2020")
    assert report["shards"][0]["missing"] == ["S100C"]

    maintenance.check(catalog.storage, repair=True, shard="2020")
    assert catalog.get_filing("S100C")["status"] == "missing"


def test_reindex_rebuilds_the_index_rows_from_the_files(catalog):
    conn = connect_write(catalog.path)
    try:
        conn.execute("DELETE FROM filings")
        conn.execute("UPDATE filing_shards SET filing_count = 0, archive_bytes = 0")
        conn.commit()
    finally:
        conn.close()

    maintenance.reindex(catalog.storage)

    assert [row["doc_id"] for row in catalog.list_recent()] == ["S100C", "S100B", "S100A"]
    assert {row["shard"]: row["filing_count"] for row in catalog.shards()} == {"2019": 1, "2020": 2}


def test_check_registers_a_yearly_file_the_index_forgot(catalog):
    conn = connect_write(catalog.path)
    try:
        conn.execute("DELETE FROM filings WHERE shard = '2019'")
        conn.execute("DELETE FROM filing_shards WHERE shard = '2019'")
        conn.commit()
    finally:
        conn.close()

    report = maintenance.check(catalog.storage)
    assert [(Path(item["file"]).name, item["shard"]) for item in report["unregistered"]] == [("filings_2019.db", "2019")]
    assert not report["ok"]

    maintenance.check(catalog.storage, repair=True)
    assert _scalar(catalog.path, "SELECT shard FROM filings WHERE doc_id = 'S100A'") == "2019"
    assert maintenance.check(catalog.storage)["ok"]


# --- reads ---------------------------------------------------------------------


def test_lists_and_coverage_open_no_yearly_file(catalog, monkeypatch, tmp_path):
    from src.web_app.api import overview

    def no_shard(*_args, **_kwargs):
        raise AssertionError("a list opened a yearly file")

    monkeypatch.setattr(FilingStorage, "open_shard", no_shard)
    monkeypatch.setattr(ShardReader, "connection", no_shard)

    assert [row["doc_id"] for row in catalog.list_recent()] == ["S100C", "S100B", "S100A"]
    assert [row["doc_id"] for row in catalog.list_company("E00001")] == ["S100B", "S100A"]
    assert catalog.coverage_summary()["unique_filings"] == 3
    catalog.quality_coverage()
    catalog.form_coverage()
    monkeypatch.setattr("src.filings.runtime.catalog", catalog)
    monkeypatch.setattr("src.filings.runtime.FILINGS_DB_PATH", catalog.path)
    monkeypatch.setattr(overview, "get_market_db", lambda: str(tmp_path / "no-market.db"))
    summary = overview.data_summary()
    assert summary["filings"]["unique_filings"] == 3
    assert summary["filing_storage"] is None


def test_shard_reader_keeps_one_connection_per_year(catalog, monkeypatch):
    import src.filings.storage as storage_module

    opened: list[str] = []
    original = storage_module.connect_read

    def recording(path, *args, **kwargs):
        opened.append(Path(path).name)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(storage_module, "connect_read", recording)
    with ShardReader(catalog.storage) as reader:
        archives = dict(reader.archives(["S100A", "S100B", "S100C", "S100NONE"]))

    assert archives["S100C"] == report_zip("S100C") and archives["S100NONE"] is None
    assert sorted(name for name in opened if name.startswith("filings_")) == ["filings_2019.db", "filings_2020.db"]


def test_a_missing_yearly_file_gives_a_clear_error(catalog):
    path = catalog.storage.shard_path("2019")
    path.rename(path.with_suffix(".moved"))

    with pytest.raises(ShardUnavailableError, match="This filing's archive is unavailable: filings_2019.db is missing"):
        catalog.get_archive_content("S100A")
    # Other years and the index keep working.
    assert catalog.get_archive_content("S100B") == report_zip("S100B")
    assert len(catalog.list_recent()) == 3
    with ShardReader(catalog.storage) as reader:
        with pytest.raises(ShardUnavailableError):
            reader.archive("S100A")
    # A write for that year is refused rather than creating an empty file.
    with pytest.raises(ShardUnavailableError):
        catalog.set_status("S100A", "parsed", None, "2026-01-01")
    assert not path.exists()
    report = maintenance.check(catalog.storage)
    assert [(item["shard"], item["state"]) for item in report["shards"] if item["shard"] == "2019"] == [("2019", "missing")]


# --- server guard ----------------------------------------------------------------


def test_the_server_cannot_write_or_create_a_yearly_file(catalog):
    shards = catalog.path.parent / "filings"
    protect_database_paths([catalog.path], [shards])
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            catalog.set_status("S100A", "error", "x", "2026-01-01")
        with pytest.raises(FilingStorageError, match="never creates filing storage"):
            catalog.upsert_filing(filing_row("S100NEW", submitted_at="2024-06-01T09:00"))
        assert not (shards / "filings_2024.db").exists()
        # Reads still work.
        assert catalog.get_archive_content("S100A") == report_zip("S100A")
    finally:
        clear_protected_database_paths()
    # A worker (no protection) creates the year's file.
    catalog.upsert_filing(filing_row("S100NEW", submitted_at="2024-06-01T09:00"))
    assert (shards / "filings_2024.db").exists()


def test_the_server_protects_the_index_every_yearly_file_and_the_folder(catalog, monkeypatch):
    from src.web_app import server

    moved = catalog.path.parent.parent / "elsewhere" / "filings_2019.db"
    moved.parent.mkdir()
    catalog.storage.shard_path("2019").rename(moved)
    maintenance.move_shard(catalog.storage, "2019", moved)
    monkeypatch.setattr(server, "get_filings_db", lambda: str(catalog.path))

    files, folders = server._worker_written_paths()

    assert str(catalog.path) in files and str(moved) in files
    assert str(catalog.path) + ".migrated" in files
    assert folders == [str(catalog.path.parent / "filings")]


# --- maintenance ---------------------------------------------------------------


def test_compact_move_and_close_years(catalog, tmp_path):
    result = maintenance.compact(catalog.storage, "2020")
    assert result["shard"] == "2020" and result["after_bytes"] > 0

    target = tmp_path / "disk2" / "filings_2019.db"
    target.parent.mkdir()
    catalog.storage.shard_path("2019").rename(target)
    with pytest.raises(FilingStorageError, match="holds 2019, not 2020"):
        maintenance.move_shard(catalog.storage, "2020", target)
    maintenance.move_shard(catalog.storage, "2019", target)
    assert catalog.storage.shard_path("2019") == target
    assert catalog.get_archive_content("S100A") == report_zip("S100A")

    from datetime import date

    closed = maintenance.close_past_years(catalog.storage, today=date(2021, 1, 15))
    assert [item["shard"] for item in closed["closed"]] == ["2019"]  # 2020 closes from February 2021
    states = {row["shard"]: row["state"] for row in catalog.shards()}
    assert states == {"2019": "closed", "2020": "active"}
    # A late write reopens a closed year.
    catalog.set_status("S100A", "parsed", None, "2026-01-01")
    assert {row["shard"]: row["state"] for row in catalog.shards()}["2019"] == "active"


def test_checkpoint_covers_the_index_and_every_year(catalog):
    from src.worker import maintenance as worker_maintenance

    done = maintenance.checkpoint_all(catalog.storage, "PASSIVE")
    assert sorted(Path(name).name for name, _ in done) == ["filings.db", "filings_2019.db", "filings_2020.db"]
    worker_maintenance.checkpoint_filings(str(catalog.path))


# --- one doorway -----------------------------------------------------------------

_GUARDED = re.compile(r"\b(archive_content|filing_archives|xbrl_facts|xbrl_contexts|xbrl_units)\b")
_ARTIFACT_SQL = re.compile(r"\b(FROM|JOIN|INTO|UPDATE|TABLE)\s+(\w+\.)?artifacts\b", re.IGNORECASE)
#: Reads the facts of one yearly file at a time through ATTACH, by design (plan §6.2).
_ALLOWED = {"src/orchestrator/generate_financial_statements/service.py"}


def test_only_the_filings_package_reads_archives_and_facts_with_sql():
    offenders = []
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        if relative.startswith("src/filings/") or relative in _ALLOWED:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _GUARDED.search(line) or _ARTIFACT_SQL.search(line):
                offenders.append(f"{relative}:{number}: {line.strip()}")
    assert offenders == []
