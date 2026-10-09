"""A company's own annual reports, apart from the reports it files for others.

Trust banks and asset managers file the reports of the trusts and funds they
run under their own EDINET code (forms such as 07A000 and 09A000), beside
their own securities report. Statement histories and multi-year windows read
only a company's own annual reports when it has any.
"""

from __future__ import annotations

COMPANY_ANNUAL_FORM_CODES = ("030000", "032000")


def own_filings_sql(alias: str, table: str, code_column: str, type_column: str = '"docTypeCode"') -> str:
    """SQL keeping the rows of *alias* (a ``FinancialStatements`` row) that are the company's own.

    *table*, *code_column* and *type_column* are quoted identifiers.
    """
    codes = ", ".join(f"'{code}'" for code in COMPANY_ANNUAL_FORM_CODES)
    return (
        f"({alias}.{type_column} IN ({codes}) OR NOT EXISTS ("
        f"SELECT 1 FROM {table} own WHERE own.{code_column} = {alias}.{code_column} "
        f"AND own.{type_column} IN ({codes})))"
    )
