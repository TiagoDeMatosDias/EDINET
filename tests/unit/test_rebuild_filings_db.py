"""Tests for rebuilding a yearly filing file from its own archives."""

from __future__ import annotations

import sys

import pytest

from scripts import rebuild_filings_db as rebuild
from src.filings.catalog import FilingCatalog
from src.filings.ingest import ingest_content
from src.orchestrator.common.sqlite import connect_read, connect_write
from tests.filing_fixtures import zip_bytes

XBRL = b"""<?xml version='1.0'?>
<xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance' xmlns:jp='https://example.test/jp'>
  <xbrli:context id='C1'><xbrli:entity><xbrli:identifier scheme='x'>E123</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2018-04-01</xbrli:startDate><xbrli:endDate>2019-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:unit id='JPY'><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jp:Revenue contextRef='C1' unitRef='JPY' decimals='0'>1234</jp:Revenue>
  <jp:Note contextRef='C1'>text, not a number</jp:Note>
</xbrli:xbrl>"""


def _catalog(tmp_path) -> FilingCatalog:
    catalog = FilingCatalog(tmp_path / "data" / "filings.db")
    for doc_id, submitted in (("S100A", "2019-06-20T09:00"), ("S100B", "2019-07-01T09:00"), ("S100C", "2020-06-20T09:00")):
        content = zip_bytes((f"XBRL/PublicDoc/{doc_id}.xbrl", XBRL))
        ingest_content(content, doc_id, catalog, {"edinet_code": "E00001", "submitted_at": submitted})
    return catalog


def _rows(path, sql: str) -> list[tuple]:
    conn = connect_read(path)
    try:
        return [tuple(row) for row in conn.execute(sql)]
    finally:
        conn.close()


def test_rebuild_shard_copies_rows_and_reparses_facts(tmp_path):
    catalog = _catalog(tmp_path)
    source = catalog.storage.shard_path("2019")
    output = tmp_path / "out" / "filings_2019.rebuilt.db"
    conn = connect_write(source)
    try:
        conn.execute(
            "INSERT INTO parse_runs (parse_run_id, doc_id, parser_version, status, started_at, fact_count) "
            "VALUES ('R1', 'S100A', 'v1', 'completed', '2026-01-01', 9)"
        )
        conn.commit()
    finally:
        conn.close()

    counts = rebuild.rebuild_shard(source, output, "2019", batch_size=1)

    assert counts["filings"] == 2 and counts["filing_archives"] == 2 and counts["damaged_archives"] == 0
    assert counts["xbrl_facts"] == 2  # one numeric fact per filing; the text fact is not kept
    assert _rows(output, "SELECT value FROM shard_info WHERE key = 'shard'") == [("2019",)]
    assert _rows(output, "SELECT doc_id, concept FROM xbrl_facts ORDER BY doc_id") == [("S100A", "Revenue"), ("S100B", "Revenue")]
    assert _rows(output, "SELECT doc_id FROM filing_archives ORDER BY doc_id") == [("S100A",), ("S100B",)]
    # Parse runs are kept, with the fact count of the re-parse.
    assert _rows(output, "SELECT parse_run_id, doc_id, fact_count FROM parse_runs") == [("R1", "S100A", 1)]
    assert not output.with_name(output.name + ".partial").exists()
    # The old file is untouched.
    assert _rows(source, "SELECT COUNT(*) FROM filing_archives") == [(2,)]


def test_rebuild_leaves_out_an_archive_that_fails_its_checksum(tmp_path, capsys):
    catalog = _catalog(tmp_path)
    source = catalog.storage.shard_path("2019")
    conn = connect_write(source)
    try:
        conn.execute("UPDATE filing_archives SET content = ? WHERE doc_id = 'S100B'", (b"damaged",))
        conn.commit()
    finally:
        conn.close()

    counts = rebuild.rebuild_shard(source, tmp_path / "filings_2019.rebuilt.db", "2019")

    assert counts["filing_archives"] == 1 and counts["damaged_archives"] == 1
    assert "S100B" in capsys.readouterr().out


def test_rebuild_refuses_a_file_holding_another_year_or_an_existing_output(tmp_path):
    catalog = _catalog(tmp_path)
    source = catalog.storage.shard_path("2019")
    with pytest.raises(SystemExit, match="not 2020"):
        rebuild.rebuild_shard(source, tmp_path / "x.db", "2020")
    existing = tmp_path / "exists.db"
    existing.write_bytes(b"")
    with pytest.raises(SystemExit, match="Refusing to overwrite"):
        rebuild.rebuild_shard(source, existing, "2019")


def test_main_dry_run_then_switch_points_the_index_at_the_new_file(tmp_path, monkeypatch, capsys):
    catalog = _catalog(tmp_path)
    old = catalog.storage.shard_path("2019")
    argv = ["rebuild_filings_db.py", "--index", str(catalog.path), "--shard", "2019"]

    monkeypatch.setattr(sys, "argv", argv)
    assert rebuild.main() == 0
    assert "Dry run" in capsys.readouterr().out
    assert not old.with_name("filings_2019.rebuilt.db").exists()

    monkeypatch.setattr(sys, "argv", [*argv, "--apply", "--switch", "--no-vacuum"])
    assert rebuild.main() == 0

    new = old.with_name("filings_2019.rebuilt.db")
    assert catalog.storage.shard_path("2019") == new
    assert old.exists()
    assert catalog.get_archive_content("S100A") is not None
    assert [row["concept"] for row in catalog.list_facts("S100A")] == ["Revenue"]
    assert {row["shard"]: row["filing_count"] for row in catalog.shards()} == {"2019": 2, "2020": 1}


def test_main_refuses_the_old_single_file(tmp_path, monkeypatch):
    from tests.filing_fixtures import create_legacy_filings_db, filing_row

    index = create_legacy_filings_db(tmp_path / "filings.db", [filing_row("S100OLD")])
    monkeypatch.setattr(sys, "argv", ["rebuild_filings_db.py", "--index", str(index)])
    with pytest.raises(SystemExit, match="split first"):
        rebuild.main()
