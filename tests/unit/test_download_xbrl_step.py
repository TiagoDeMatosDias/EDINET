"""Pipeline XBRL step contract tests."""

from src.orchestrator.download_xbrl.download_xbrl import _document_ids


def test_xbrl_step_bounds_and_normalizes_document_ids():
    assert _document_ids({"document_ids": " A1, B2 ,, C3 "}) == ["A1", "B2", "C3"]
    assert len(_document_ids({"document_ids": [str(i) for i in range(200)]})) == 100
