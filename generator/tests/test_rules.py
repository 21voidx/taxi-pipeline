from __future__ import annotations

import random
from datetime import datetime

from ride_generator.rules import calculate_fare, demand_multiplier, is_peak_hour


def test_weekday_morning_is_peak() -> None:
    assert is_peak_hour(datetime(2026, 8, 3, 8, 0, 0)) is True


def test_weekend_morning_is_not_commuter_peak() -> None:
    assert is_peak_hour(datetime(2026, 8, 2, 8, 0, 0)) is False


def test_jakarta_peak_has_larger_demand_multiplier() -> None:
    peak = demand_multiplier(datetime(2026, 8, 3, 8), "JKT", "CBD")
    night = demand_multiplier(datetime(2026, 8, 3, 3), "JKT", "CBD")
    assert peak > night


def test_final_fare_is_non_negative() -> None:
    fare = calculate_fare("BIKE", 5.0, 20.0, 1.2, random.Random(42))
    assert fare.final_fare >= 0
    assert fare.gross_fare >= fare.final_fare
