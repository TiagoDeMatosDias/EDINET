"""Point-in-time selection and execution-cost tests."""

from datetime import date, datetime, timezone

from src.backtesting.asof import ExecutionCostModel, HistoricalObservation, select_as_of


def test_as_of_selection_uses_submission_and_withdrawal_dates():
    early = HistoricalObservation("E1", "Revenue", date(2024, 3, 31), 100, datetime(2024, 5, 1, tzinfo=timezone.utc), datetime(2024, 5, 1, tzinfo=timezone.utc), source_id="early")
    amendment = HistoricalObservation("E1", "Revenue", date(2024, 3, 31), 110, datetime(2024, 6, 1, tzinfo=timezone.utc), datetime(2024, 6, 1, tzinfo=timezone.utc), source_id="amendment")
    assert select_as_of([early, amendment], datetime(2024, 5, 15, tzinfo=timezone.utc))[('E1', 'Revenue', date(2024, 3, 31))].value == 100
    assert select_as_of([early, amendment], datetime(2024, 6, 15, tzinfo=timezone.utc))[('E1', 'Revenue', date(2024, 3, 31))].value == 110


def test_execution_costs_are_adverse_by_side():
    model = ExecutionCostModel(commission_bps=10, slippage_bps=20, spread_bps=10)
    assert model.fill_price(100, "buy") > 100
    assert model.fill_price(100, "sell") < 100
    assert model.commission(10_000) == 10
