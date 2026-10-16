"""Splitting the old single filings.db into the index and yearly files, and switching over."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

import pytest

from src.filings import maintenance
from src.filings import split as splitting
from src.filings.catalog import FilingCatalog
from src.filings.storage import FilingStorage
from tests.filing_fixtures import create_legacy_filings_db, filing_row, report_zip

YEARS = {"2019": 3, "2020": 2, "2021": 4}
PER_DOC_TABLES = ("artifacts", "xbrl_contexts", "xbrl_units", "xbrl_facts", "quality_issues", "parse_runs")


def _filings() -> list[dict]:
    rows = []
    for year, count in YEARS.items():
        for number in range(count):
            # Company reports (030000), fund reports (07A000) and two date formats.
            form = "07A000" if number == 1 else "030000"
            when = f"{year}-06-{10 + number:02d}" + ("T09:00" if number % 2 else " 15:00")
            rows.append(filing_row(f"S{year}{number:04d}", f"E{number:05d}", when, form_code=form))
    # A filing without a retained archive.
    rows.append(filing_row("S2020NOZIP", "E00009", "2020-09-01T09:00", content=None))
    return rows


@pytest.fixture
def old(tmp_path) -> Path:
    return create_legacy_filings_db(tmp_path / "data" / "filings.db", _filings())


def _count(path: Path, sql: str, params: tuple = ()) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute(sql, params).fetchone()[0])
    finally:
        conn.close()


def _source_counts(path: Path) -> dict[str, int]:
    return {table: _count(path, f"SELECT COUNT(*) FROM {table}") for table in ("filings", *PER_DOC_TABLES)}


def _shard_counts(storage: FilingStorage) -> dict[str, int]:
    totals = dict.fromkeys(("filings", *PER_DOC_TABLES), 0)
    for row in storage.shards():
        for table in totals:
            totals[table] += _count(Path(row["file"]), f"SELECT COUNT(*) FROM {table}")
    return totals


def test_preflight_lists_the_years_files_and_space(old):
    plan = splitting.preflight(old)

    assert plan["needed"] and plan["layout"] == "legacy"
    assert plan["filings"] == sum(YEARS.values()) + 1
    assert [(year["shard"], year["filings"]) for year in plan["years"]] == [("2019", 3), ("2020", 3), ("2021", 4)]
    assert plan["default_folder"] and plan["target_folder"] == str(old.parent / "filings")
    assert [Path(name).name for name in plan["files"]] == ["filings.next.db", "filings_2019.db", "filings_2020.db", "filings_2021.db"]
    assert plan["needed_bytes"] >= old.stat().st_size and plan["enough_space"]
    assert plan["undated_count"] == 0
    # Nothing was written.
    assert not (old.parent / "filings").exists() and not (old.parent / "filings.next.db").exists()
    assert splitting.notice(old)["state"] == "legacy"


def test_split_copies_everything_then_the_switch_over_renames_the_files(old):
    source = _source_counts(old)
    digest = hashlib.sha256(old.read_bytes()).hexdigest()

    result = splitting.split(old, chunk_size=2)

    assert result["state"] == "ready" and [year["shard"] for year in result["years"]] == ["2019", "2020", "2021"]
    # The old file is untouched and still the live one until the switch-over.
    assert hashlib.sha256(old.read_bytes()).hexdigest() == digest
    assert FilingCatalog(old).layout == "legacy"
    assert splitting.status(old)["state"] == "ready"
    assert splitting.notice(old)["state"] == "ready"
    upcoming = FilingStorage(old.parent / "filings.next.db")
    assert _shard_counts(upcoming) == source
    assert result["tables"]["filing_translations"] == 1 and result["tables"]["Bond_Documents"] == 1

    assert splitting.switch_over(old)["status"] == "switched"

    assert (old.parent / "filings.db.migrated").exists() and not (old.parent / "filings.next.db").exists()
    catalog = FilingCatalog(old)
    assert catalog.layout == "split-v1"
    assert splitting.status(old)["state"] == "split" and splitting.notice(old) is None
    assert {row["shard"]: row["filing_count"] for row in catalog.shards()} == {"2019": 3, "2020": 3, "2021": 4}
    assert catalog.get_archive_content("S20200001") == report_zip("S20200001")
    assert catalog.get_archive_content("S2020NOZIP") is None
    assert len(catalog.list_facts("S20210003")) == 2
    assert catalog.coverage_summary()["filings_with_issues"] == sum(YEARS.values()) + 1
    assert _count(catalog.path, "SELECT COUNT(*) FROM filing_translations WHERE translated_text = 'Net sales'") == 1
    assert _count(catalog.path, "SELECT COUNT(*) FROM Bond_Documents") == 1
    assert _count(catalog.path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'idx_bond_documents_company'") == 1
    assert maintenance.check(catalog.storage)["ok"]
    # Writes go to the yearly files now.
    catalog.upsert_filing(filing_row("S2022NEW", submitted_at="2022-01-04T09:00"))
    assert (old.parent / "filings" / "filings_2022.db").exists()

    freed = splitting.delete_retired(old)
    assert freed > 0 and not (old.parent / "filings.db.migrated").exists()


def test_split_and_switch_in_one_go_from_the_command_line(old):
    result = splitting.split(old, switch=True)
    assert result["state"] == "split" and result["switch"]["status"] == "switched"
    assert FilingCatalog(old).layout == "split-v1"


def test_an_interrupted_split_resumes_where_it_stopped(old):
    calls = 0

    def stop_after_three_chunks():
        nonlocal calls
        calls += 1
        if calls == 4:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        splitting.split(old, chunk_size=1, checkpoint=stop_after_three_chunks)

    assert splitting.status(old)["state"] == "splitting"
    assert splitting.notice(old)["state"] == "splitting"
    assert _count(old.parent / "filings" / "filings_2019.db", "SELECT COUNT(*) FROM filings") == 3

    result = splitting.split(old, chunk_size=1)

    assert result["years"][0]["copied"] == 0  # 2019 was complete
    assert sum(year["copied"] for year in result["years"]) == sum(YEARS.values()) + 1 - 3
    assert _shard_counts(FilingStorage(old.parent / "filings.next.db")) == _source_counts(old)


def test_a_corrupted_archive_stops_the_split_and_names_the_filing(old):
    conn = sqlite3.connect(old)
    conn.execute("UPDATE filings SET archive_sha256 = 'not-the-hash' WHERE doc_id = 'S20200001'")
    conn.commit()
    conn.close()

    with pytest.raises(splitting.SplitError, match="S20200001"):
        splitting.split(old)
    assert FilingCatalog(old).layout == "legacy"
    assert splitting.switch_over(old)["status"] == "copying"


def test_a_missing_row_is_caught_and_the_year_copied_anew(old, monkeypatch):
    original = splitting._copy_year

    def losing_a_fact(source, path, shard, doc_ids, **kwargs):
        copied = original(source, path, shard, doc_ids, **kwargs)
        if shard == "2020":
            conn = sqlite3.connect(path)
            conn.execute("DELETE FROM xbrl_facts WHERE fact_id = 'S20200000-F1'")
            conn.commit()
            conn.close()
        return copied

    monkeypatch.setattr(splitting, "_copy_year", losing_a_fact)
    with pytest.raises(splitting.SplitError, match="xbrl_facts 5 of 6"):
        splitting.split(old)
    assert not (old.parent / "filings" / "filings_2020.db").exists()

    monkeypatch.setattr(splitting, "_copy_year", original)
    assert splitting.split(old)["state"] == "ready"
    assert _shard_counts(FilingStorage(old.parent / "filings.next.db")) == _source_counts(old)


def test_undated_filings_are_refused_before_anything_is_written(tmp_path):
    old = create_legacy_filings_db(tmp_path / "filings.db", [filing_row("S100OK"), filing_row("S100NODATE", submitted_at="")])

    plan = splitting.preflight(old)
    assert plan["undated"] == ["S100NODATE"]
    with pytest.raises(splitting.SplitError, match="S100NODATE"):
        splitting.split(old)
    assert not (tmp_path / "filings.next.db").exists()


def test_a_foreign_yearly_file_is_not_overwritten(old):
    folder = old.parent / "filings"
    folder.mkdir()
    (folder / "filings_2019.db").write_bytes(b"someone else's file")

    with pytest.raises(splitting.SplitError, match="not part of this split"):
        splitting.split(old)
    assert (folder / "filings_2019.db").read_bytes() == b"someone else's file"


def test_a_failed_rename_leaves_the_old_layout_in_place(old, monkeypatch):
    splitting.split(old)
    real_rename = os.rename
    renamed: list[str] = []

    def failing(src, dst):
        if Path(src).name == "filings.next.db":
            raise PermissionError("the file is open in another process")
        renamed.append(Path(src).name)
        real_rename(src, dst)

    monkeypatch.setattr(splitting.os, "rename", failing)
    outcome = splitting.switch_over(old)
    monkeypatch.undo()

    assert outcome["status"] == "failed" and "open in another process" in outcome["error"]
    assert "filings.db" in renamed  # it was moved, then put back
    assert FilingCatalog(old).layout == "legacy"
    assert not (old.parent / "filings.db.migrated").exists()
    assert splitting.status(old)["state"] == "ready"
    # The next start succeeds.
    assert splitting.switch_over(old)["status"] == "switched"


def test_the_switch_over_keeps_step_logs_written_after_the_split(old):
    splitting.split(old)
    # A bond run between the split and the restart logs its work in the old file.
    conn = sqlite3.connect(old)
    conn.execute("INSERT INTO Bond_Documents VALUES ('S100LATE', 'E00002', NULL, '2026-10-16T12:00:00+00:00')")
    conn.commit()
    conn.close()

    assert splitting.switch_over(old)["status"] == "switched"

    assert _count(old, "SELECT COUNT(*) FROM Bond_Documents") == 2
    assert _count(old, "SELECT COUNT(*) FROM Bond_Documents WHERE doc_id = 'S100LATE'") == 1


def test_the_server_start_finishes_a_ready_split(old, monkeypatch):
    from src.web_app import server

    splitting.split(old)
    monkeypatch.setattr(server, "get_filings_db", lambda: str(old))
    server._finish_filing_storage_split()
    assert FilingCatalog(old).layout == "split-v1"
    server._finish_filing_storage_split()  # nothing left to do
    assert FilingCatalog(old).layout == "split-v1"


def test_rolling_back_renames_the_old_file_into_place(old):
    splitting.split(old, switch=True)
    split_index = old.with_name("filings.split.db")
    os.rename(old, split_index)
    for suffix in ("-wal", "-shm"):
        old.with_name(old.name + suffix).unlink(missing_ok=True)
    os.rename(old.with_name("filings.db.migrated"), old)

    catalog = FilingCatalog(old)
    assert catalog.layout == "legacy"
    assert catalog.get_archive_content("S20190000") == report_zip("S20190000")
    assert splitting.status(old)["state"] == "legacy"
