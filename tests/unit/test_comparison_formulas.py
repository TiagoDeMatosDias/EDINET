from __future__ import annotations

import pytest

from src.comparison.formulas import PRICE_REF, evaluate_formula, formula_columns, parse_formula

EPS = "ShareMetrics.Basic earnings (loss) per share"


def _value(values: dict[str, float | None]):
    return lambda reference: values.get(reference)


def test_a_formula_reads_bracketed_columns_numbers_and_operators():
    tokens = parse_formula(f"=[{PRICE_REF}] / [{EPS}]")

    assert tokens == [("column", PRICE_REF), ("op", "/"), ("column", EPS)]
    assert formula_columns(tokens) == [PRICE_REF, EPS]
    assert evaluate_formula(tokens, _value({PRICE_REF: 3000, EPS: 250})) == 12


def test_multiplication_binds_first_and_parentheses_group():
    values = _value({"A.x": 2, "B.y": 3})

    assert evaluate_formula(parse_formula("=[A.x] + [B.y] * 4"), values) == 14
    assert evaluate_formula(parse_formula("=([A.x] + [B.y]) * 4"), values) == 20
    assert evaluate_formula(parse_formula("=[A.x] - 0.5"), values) == 1.5
    assert evaluate_formula(parse_formula("=[A.x] * -0.5"), values) == -1
    assert evaluate_formula(parse_formula("=[A.x] * 1e3"), values) == 2000


def test_a_doubled_bracket_is_part_of_the_column_name():
    tokens = parse_formula("=[Table.Odd ]] name] * 2")

    assert formula_columns(tokens) == ["Table.Odd ] name"]


def test_a_missing_value_or_a_division_by_zero_leaves_the_result_empty():
    tokens = parse_formula("=[A.x] / [B.y]")

    assert evaluate_formula(tokens, _value({"A.x": 2, "B.y": 0})) is None
    assert evaluate_formula(tokens, _value({"A.x": 2})) is None
    assert evaluate_formula(parse_formula("=([A.x] + 1) * 0 + [B.y]"), _value({"B.y": 5})) is None


@pytest.mark.parametrize("metric", [
    "PERatio",
    "IncomeStatement.Net sales",
    "=",
    "=2 * 3",
    "=[A.x] [B.y]",
    "=[A.x] *",
    "=([A.x]",
    "=[A.x])",
    "=[Ax]",
    "=[A.x] ^ 2",
    "=[A.x",
    "=" + " + ".join(["[A.x]"] * 40),
])
def test_anything_else_is_not_a_formula(metric):
    assert parse_formula(metric) is None


def _overview(price: float) -> dict:
    return {"company": {"company_name": "Alpha"}, "market": {"latest_price": price}}


def test_the_snapshot_evaluates_formula_metrics_from_columns_and_the_latest_price(monkeypatch):
    import src.comparison.api as comparison_api

    formula = f"=[{EPS}] / [{PRICE_REF}]"
    unknown = "=[Elsewhere.Column] * 2"
    overviews = {"E1": _overview(1000), "E2": _overview(2000)}
    eps = {"E1": 50, "E2": None}
    read: list[list[str]] = []

    def column_values(_db, code, references):
        read.append(references)
        return {reference: eps[code] if reference == EPS else None for reference in references}, "2026-03-31"

    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(comparison_api, "_metric_catalog", lambda _db: {"ShareMetrics": ["Basic earnings (loss) per share"]})
    monkeypatch.setattr(comparison_api, "get_security_overview", lambda _db, company_code: overviews[company_code])
    monkeypatch.setattr(comparison_api, "_enrich_overview", lambda _db, _code, overview: overview)
    monkeypatch.setattr(comparison_api, "_custom_metric_values", column_values)

    response = comparison_api.snapshot(comparison_api.ComparisonRequest(
        company_codes=["E1", "E2"], metrics=["LatestPrice", formula, unknown],
    ))

    # A formula over a column the comparison cannot read is dropped like an unknown column.
    assert response["metrics"] == ["LatestPrice", formula]
    # The formula's column is read even though it is not shown on its own.
    assert read[0] == [EPS]
    assert response["companies"][0]["metrics"][formula] == 0.05
    assert response["companies"][1]["metrics"][formula] is None
    assert response["metric_definitions"][formula] == {"label": formula[1:], "group": "Formula"}


def test_statement_columns_carry_the_screeners_display_format(monkeypatch):
    import src.comparison.api as comparison_api

    monkeypatch.setattr(comparison_api, "taxonomy_formats", lambda _db: {"ShareMetrics.Payout ratio": "percent"})
    growth = "IncomeStatement_Rolling.Net sales_Growth_3_Year"
    metrics = ["PERatio", "ShareMetrics.Payout ratio", "Financial_Ratios.Return on Equity", growth, f"=[{EPS}] / [{PRICE_REF}]", EPS]

    formats = comparison_api._column_formats("fixture.db", metrics)

    assert formats == {"ShareMetrics.Payout ratio": "percent", "Financial_Ratios.Return on Equity": "percent", growth: "percent"}
    assert comparison_api._metric_definition("Financial_Ratios.Return on Equity", formats) == {
        "label": "Return on Equity", "group": "Financial_Ratios", "format": "percent",
    }
    assert comparison_api._metric_definition(EPS, formats) == {"label": "Basic earnings (loss) per share", "group": "ShareMetrics"}
