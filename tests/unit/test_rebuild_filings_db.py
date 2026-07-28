"""Tests for the compact filing database rebuild helpers."""

from __future__ import annotations

from scripts.rebuild_filings_db import _copy_catalog_rows
from src.filings.catalog import FilingCatalog
from src.orchestrator.common.sqlite import connect_read, connect_write


def test_copy_catalog_rows_copies_artifact_metadata_without_content(tmp_path):
    source_path = tmp_path / "source.db"
    destination_path = tmp_path / "destination.db"
    FilingCatalog(source_path)
    FilingCatalog(destination_path)

    source = connect_write(source_path)
    try:
        source.execute(
            """INSERT INTO filings(
                doc_id, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?)""",
            ("S100TEST", "parsed", "2026-01-01", "2026-01-01"),
        )
        source.execute(
            """INSERT INTO artifacts(
                artifact_id, doc_id, member_path, kind, media_type,
                size_bytes, sha256, content
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "artifact-1",
                "S100TEST",
                "PublicDoc/report.xbrl",
                "xbrl",
                "application/xml",
                10,
                "digest",
                b"legacy extracted content",
            ),
        )
        source.commit()
    finally:
        source.close()

    source = connect_read(source_path)
    destination = connect_write(destination_path)
    try:
        counts = _copy_catalog_rows(source, destination, batch_size=10)
    finally:
        source.close()
        destination.close()

    destination = connect_read(destination_path)
    try:
        artifact = destination.execute(
            "SELECT artifact_id, sha256, content FROM artifacts"
        ).fetchone()
    finally:
        destination.close()

    assert counts["filings"] == 1
    assert counts["artifacts"] == 1
    assert tuple(artifact) == ("artifact-1", "digest", None)
