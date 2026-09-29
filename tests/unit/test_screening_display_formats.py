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
