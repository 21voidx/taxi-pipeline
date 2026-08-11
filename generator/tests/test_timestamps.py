from __future__ import annotations

from datetime import date, datetime

from ride_generator.config import Settings
from ride_generator.service import (
    RideGenerator,
    jakarta_naive_from_utc,
    utc_naive_from_jakarta,
)


def test_timezone_round_trip() -> None:
    jakarta = datetime(2026, 8, 7, 8, 30, 15)
    assert jakarta_naive_from_utc(utc_naive_from_jakarta(jakarta)) == jakarta


def test_historical_request_timestamps_are_unique_and_sorted() -> None:
    generator = RideGenerator(Settings(seed=42))
    values = generator._historical_requested_times(date(2026, 8, 6), 250)
    assert values == sorted(values)
    assert len(values) == 250
    assert len(set(values)) == 250
    assert values[0].date() == date(2026, 8, 6)
    assert values[-1].date() == date(2026, 8, 6)


def test_stable_lifecycle_delays_are_reproducible() -> None:
    generator = RideGenerator(Settings(seed=42))
    assert generator._accept_delay_seconds(1001) == generator._accept_delay_seconds(1001)
    assert generator._arrival_delay_minutes(1001, "BIKE") == generator._arrival_delay_minutes(1001, "BIKE")
    assert 8 <= generator._accept_delay_seconds(1001) <= 90
    assert 2.5 <= generator._arrival_delay_minutes(1001, "BIKE") <= 13.0
