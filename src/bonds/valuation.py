"""Bond arithmetic and the government yield curve.

Prices are per 100 of face value and yields compound at the coupon
frequency, the same conventions as the browser calculator
(``frontend-v2/src/features/research/pricing/bondModel.ts``). The Ministry of
Finance publishes Japanese government bond (JGB) par yields for 1 to 40
years; between those tenors the curve is linear and beyond them it is flat.
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Iterable
from datetime import date

DAYS_PER_YEAR = 365.25


def year_fraction(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        return (date.fromisoformat(end[:10]) - date.fromisoformat(start[:10])).days / DAYS_PER_YEAR
    except ValueError:
        return None


def cashflow_times(years: float, frequency: int) -> list[float]:
    """Coupon dates in years from now; a fractional maturity has a short first period."""
    if not (years > 0) or not (frequency > 0):
        return []
    count = max(1, math.ceil(years * frequency - 1e-9))
    return [years - (count - 1 - index) / frequency for index in range(count)]


def accrued(coupon: float, years: float, frequency: int) -> float:
    times = cashflow_times(years, frequency)
    if not times:
        return 0.0
    return max(1 - times[0] * frequency, 0.0) * 100 * coupon / frequency


def dirty_price(coupon: float, years: float, rate: float, frequency: int = 2) -> float:
    payment = 100 * coupon / frequency
    times = cashflow_times(years, frequency)
    total = sum(payment * (1 + rate / frequency) ** (-frequency * time) for time in times)
    return total + (100 * (1 + rate / frequency) ** (-frequency * years) if times else 0.0)


def clean_price(coupon: float, years: float, rate: float, frequency: int = 2) -> float:
    return dirty_price(coupon, years, rate, frequency) - accrued(coupon, years, frequency)


def yield_from_price(coupon: float, years: float, price: float, frequency: int = 2) -> float | None:
    """Yield to maturity for a clean price, or ``None`` if no yield between -50% and 200% gives it."""
    if not (years > 0) or not (price > 0):
        return None
    target = price + accrued(coupon, years, frequency)
    low, high = -0.5, 2.0
    if target > dirty_price(coupon, years, low, frequency) or target < dirty_price(coupon, years, high, frequency):
        return None
    for _ in range(200):
        middle = (low + high) / 2
        if dirty_price(coupon, years, middle, frequency) > target:
            low = middle
        else:
            high = middle
        if high - low < 1e-10:
            break
    return (low + high) / 2


def modified_duration(coupon: float, years: float, rate: float, frequency: int = 2) -> float | None:
    price = dirty_price(coupon, years, rate, frequency)
    if not (price > 0):
        return None
    payment = 100 * coupon / frequency
    times = cashflow_times(years, frequency)
    weighted = sum(time * (payment + (100 if index == len(times) - 1 else 0)) * (1 + rate / frequency) ** (-frequency * time) for index, time in enumerate(times))
    return weighted / price / (1 + rate / frequency)


class Curve:
    """One day's par-yield curve."""

    def __init__(self, curve_date: str, points: Iterable[tuple[float, float]]):
        ordered = sorted(points)
        self.date = curve_date
        self.tenors = [tenor for tenor, _ in ordered]
        self.yields = [value for _, value in ordered]

    def at(self, years: float | None) -> float | None:
        if years is None or not self.tenors:
            return None
        if years <= self.tenors[0]:
            return self.yields[0]
        if years >= self.tenors[-1]:
            return self.yields[-1]
        index = bisect.bisect_right(self.tenors, years)
        left, right = self.tenors[index - 1], self.tenors[index]
        weight = (years - left) / (right - left)
        return self.yields[index - 1] + weight * (self.yields[index] - self.yields[index - 1])

    def points(self) -> list[dict[str, float]]:
        return [{"tenor": tenor, "yield": value} for tenor, value in zip(self.tenors, self.yields, strict=True)]


class CurveBook:
    """Every stored curve; ``on(day)`` is the latest curve published on or before ``day``."""

    def __init__(self, rows: Iterable[tuple[str, float, float]]):
        by_date: dict[str, list[tuple[float, float]]] = {}
        for curve_date, tenor, value in rows:
            by_date.setdefault(str(curve_date), []).append((float(tenor), float(value)))
        self.dates = sorted(by_date)
        self._points = by_date
        self._cache: dict[str, Curve] = {}

    def __bool__(self) -> bool:
        return bool(self.dates)

    def on(self, day: str | None) -> Curve | None:
        if not day or not self.dates:
            return None
        index = bisect.bisect_right(self.dates, day[:10]) - 1
        if index < 0:
            return None
        curve_date = self.dates[index]
        if curve_date not in self._cache:
            self._cache[curve_date] = Curve(curve_date, self._points[curve_date])
        return self._cache[curve_date]

    def latest(self) -> Curve | None:
        return self.on(self.dates[-1]) if self.dates else None

    @classmethod
    def load(cls, conn) -> "CurveBook":
        try:
            rows = conn.execute("SELECT curve_date, tenor, yield FROM JGB_Yields").fetchall()
        except Exception:  # noqa: BLE001 - a missing table means no curve yet
            rows = []
        return cls((row[0], row[1], row[2]) for row in rows)
