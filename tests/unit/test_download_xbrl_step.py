"""Pipeline XBRL step contract tests."""

from src.filings.acquisition import EdinetAcquisitionError
from src.orchestrator.common.sqlite import connect_write
from src.orchestrator.download_xbrl import download_xbrl as download_step


def _create_base(path, *, with_xbrl_status=True):
    conn = connect_write(path)
    try:
        status_column = ', XbrlDownloaded TEXT NOT NULL DEFAULT "False"' if with_xbrl_status else ""
        conn.execute(
            "CREATE TABLE DocumentList ("
            "docID TEXT PRIMARY KEY, edinetCode TEXT, filerName TEXT, "
            "submitDateTime TEXT, periodStart TEXT, periodEnd TEXT, formCode TEXT, "
            "docTypeCode TEXT, xbrlFlag TEXT, legalStatus TEXT, Downloaded TEXT"
            f"{status_column})"
        )
        conn.executemany(
            "INSERT INTO DocumentList "
            "(docID, edinetCode, filerName, submitDateTime, periodStart, periodEnd, "
            "formCode, docTypeCode, xbrlFlag, legalStatus, Downloaded) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("A", "E00001", "A Co", "2025-03-03", "2024-04-01", "2025-03-31", "030", "120", "1", "1", "True"),
                ("B", "E00002", "B Co", "2025-03-02", "2024-04-01", "2025-03-31", "030", "130", "1", "2", "True"),
                ("C", "E00003", "C Co", "2025-03-01", "2024-04-01", "2025-03-31", "030", "120", "1", "1", "True"),
                ("D", "E00004", "D Co", "2025-02-28", "2024-04-01", "2025-03-31", "030", "140", "1", "1", "True"),
                ("E", "E00005", "E Co", "2025-02-27", "2024-04-01", "2025-03-31", "030", "120", "0", "1", "True"),
                ("F", "E00006", "F Co", "2025-02-26", "2024-04-01", "2025-03-31", "030", "120", "1", "0", "True"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _status_rows(path):
    conn = connect_write(path)
    try:
        return {
            row["docID"]: row["XbrlDownloaded"]
            for row in conn.execute(
                "SELECT docID, XbrlDownloaded FROM DocumentList ORDER BY docID"
            )
        }
    finally:
        conn.close()


def test_xbrl_step_bounds_and_normalizes_document_ids():
    assert download_step._document_ids({"document_ids": " A1, B2 ,, C3 "}) == ["A1", "B2", "C3"]
    assert len(download_step._document_ids({"document_ids": [str(i) for i in range(200)]})) == 100


def test_all_mode_adds_status_column_and_selects_eligible_documents(tmp_path, monkeypatch):
    base_path = tmp_path / "Base.db"
    _create_base(base_path, with_xbrl_status=False)

    from src.orchestrator.common import db_config

    monkeypatch.setattr(db_config, "get_db1", lambda: str(base_path))

    assert download_step._all_ids({"doc_type_code": "120"}) == ["A", "C"]
    conn = connect_write(base_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(DocumentList)").fetchall()}
    finally:
        conn.close()
    assert "XbrlDownloaded" in columns

    conn = connect_write(base_path)
    try:
        conn.execute("UPDATE DocumentList SET XbrlDownloaded = 'True' WHERE docID = 'C'")
        conn.commit()
    finally:
        conn.close()

    assert download_step._all_ids({"doc_type_code": ""}) == ["A", "B", "D"]


def test_all_mode_updates_base_statuses_and_retries_failures(tmp_path, monkeypatch):
    base_path = tmp_path / "Base.db"
    _create_base(base_path)

    from src.filings import acquisition, runtime
    from src.orchestrator.common import db_config

    monkeypatch.setattr(db_config, "get_db1", lambda: str(base_path))

    class FakeCatalog:
        def get_filing(self, document_id):
            if document_id == "A":
                return {"status": "parsed"}
            return None

    calls = []

    class FakeClient:
        def __init__(self, token):
            assert token == "token"

        def acquire_type1(self, document_id, catalog, metadata):
            calls.append((document_id, metadata))
            if document_id == "C":
                raise EdinetAcquisitionError("not available")
            if document_id == "D":
                raise RuntimeError("temporary failure")

    monkeypatch.setattr(acquisition, "EdinetDownloadClient", FakeClient)
    monkeypatch.setattr(runtime, "catalog", FakeCatalog())

    result = download_step.run_download_xbrl(
        {
            "download_xbrl_config": {
                "mode": "all",
                "doc_type_code": "",
                "provider_token": "token",
            }
        }
    )

    assert result == {
        "mode": "all",
        "candidates": 4,
        "downloaded": 1,
        "skipped": 1,
        "failed": 2,
    }
    assert [document_id for document_id, _ in calls] == ["B", "C", "D"]
    assert calls[0][1]["edinet_code"] == "E00002"
    assert _status_rows(base_path) == {
        "A": "True",
        "B": "True",
        "C": "Checked_Unavailable",
        "D": "Checked_Error",
        "E": "False",
        "F": "False",
    }
