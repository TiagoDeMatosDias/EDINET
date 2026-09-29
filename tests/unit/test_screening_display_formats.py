"""Screening column formats come from the pipelines that define the columns."""

from src.screening.display_formats import column_format, result_column_formats


def test_ratio_definitions_declare_percent_columns():
    assert column_format("Financial_Ratios.Return on Equity") == "percent"
    assert column_format("Financial_Ratios.Current Ratio") is None
    assert column_format("PerShare_Metrics.Earnings Per Share") is None


def test_rolling_columns_inherit_or_declare_their_format():
    assert column_format("Financial_Ratios_Rolling.Return on Equity_Average_3_Year") == "percent"
    assert column_format("Financial_Ratios_Rolling.Current Ratio_Average_3_Year") is None
    assert column_format("IncomeStatement_Rolling.Net sales_Growth_5_Year") == "percent"


def test_formats_are_keyed_by_result_column_names():
    formats = result_column_formats([
        "CompanyInfo.Company_Name",
        "Financial_Ratios.Return on Equity",
        "Financial_Ratios_Rolling.Return on Equity_Average_3_Year",
    ])

    assert formats == {
        "Return on Equity": "percent",
        "Return on Equity_Average_3_Year": "percent",
    }


def test_taxonomy_percent_items_are_formatted_as_percentages(tmp_path):
    import sqlite3

    database = tmp_path / "standardized.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE Statement_Hierarchy (statement_family TEXT, concept_qname TEXT, primary_label_en TEXT, is_column INTEGER)")
        conn.execute("CREATE TABLE Taxonomy_Dictionary (release_id TEXT, concept_qname TEXT, item_type TEXT)")
        conn.executemany("INSERT INTO Statement_Hierarchy VALUES (?, ?, ?, 1)", [
            ("ShareMetrics", "jpcrp_cor:EquityToAssetRatioSummaryOfBusinessResults", "Equity-to-asset ratio"),
            ("ShareMetrics", "jpcrp_cor:NumberOfEmployees", "Number of employees"),
        ])
        conn.executemany("INSERT INTO Taxonomy_Dictionary VALUES ('2025-11-01', ?, ?)", [
            ("jpcrp_cor:EquityToAssetRatioSummaryOfBusinessResults", "num:percentItemType"),
            ("jpcrp_cor:NumberOfEmployees", "xbrli:sharesItemType"),
        ])

    formats = result_column_formats(
        ["ShareMetrics.Equity-to-asset ratio", "ShareMetrics.Number of employees", "ShareMetrics_Rolling.Equity-to-asset ratio_Average_5_Year"],
        str(database),
    )

    assert formats == {
        "Equity-to-asset ratio": "percent",
        "Equity-to-asset ratio_Average_5_Year": "percent",
    }


def test_missing_concept_dictionary_leaves_columns_unformatted(tmp_path):
    import sqlite3

    database = tmp_path / "standardized.db"
    sqlite3.connect(database).close()

    assert result_column_formats(["ShareMetrics.Equity-to-asset ratio"], str(database)) == {}
