import sqlite3

from src.security_analysis.security_analysis import get_security_statements


def _wide_columns(prefix: str, count: int = 1_000) -> str:
    return ", ".join(f'"{prefix}_{index}" REAL' for index in range(count))


def test_statement_history_queries_wide_sources_independently(tmp_path) -> None:
    db_path = tmp_path / "wide-history.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE CompanyInfo ("
            "EdinetCode TEXT, Company_Ticker TEXT, Company_Name TEXT)"
        )
        conn.execute("CREATE TABLE Stock_Prices (Ticker TEXT, Date TEXT, Price REAL)")
        conn.execute(
            "CREATE TABLE FinancialStatements ("
            "docID TEXT, edinetCode TEXT, periodEnd TEXT)"
        )
        conn.execute(f"CREATE TABLE WideA (docID TEXT, {_wide_columns('a')})")
        conn.execute(f"CREATE TABLE WideB (docID TEXT, {_wide_columns('b')})")
        conn.execute(
            "INSERT INTO FinancialStatements VALUES (?, ?, ?)",
            ("doc-1", "E00001", "2025-03-31"),
        )
        conn.execute('INSERT INTO WideA (docID, "a_0") VALUES (?, ?)', ("doc-1", 10))
        conn.execute('INSERT INTO WideB (docID, "b_0") VALUES (?, ?)', ("doc-1", 20))

    result = get_security_statements(
        str(db_path),
        "E00001",
        periods=1,
        statement_sources={"Wide A": "WideA", "Wide B": "WideB"},
    )

    assert result["periods"] == ["2025-03-31"]
    assert result["WideA"][0]["values"] == [10.0]
    assert result["WideB"][0]["values"] == [20.0]

