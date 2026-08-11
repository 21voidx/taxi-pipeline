from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal
from typing import Any

from faker import Faker
from psycopg import Connection
from psycopg.rows import dict_row

from .config import Settings
from .database import connect
from .reference import CITIES, CITY_CODES, CITY_WEIGHTS, ZONES
from .rules import (
    acceptance_probability,
    calculate_fare,
    cancellation_probability,
    choose_service_type,
    duration_from_distance,
    surge_multiplier,
)

LOGGER = logging.getLogger(__name__)
UTC = ZoneInfo("UTC")
JAKARTA = ZoneInfo("Asia/Jakarta")


def utc_now_naive() -> datetime:
    """Return current UTC as a naive timestamp for PostgreSQL TIMESTAMP columns."""
    return datetime.now(UTC).replace(tzinfo=None)


def utc_naive_from_jakarta(moment: datetime) -> datetime:
    """Convert a naive Jakarta business timestamp to a naive UTC storage timestamp."""
    return moment.replace(tzinfo=JAKARTA).astimezone(UTC).replace(tzinfo=None)


def jakarta_naive_from_utc(moment: datetime) -> datetime:
    """Convert a naive UTC storage timestamp to a naive Jakarta business timestamp."""
    return moment.replace(tzinfo=UTC).astimezone(JAKARTA).replace(tzinfo=None)


def jakarta_now_naive() -> datetime:
    return datetime.now(JAKARTA).replace(tzinfo=None)

class RideGenerator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.rng = random.Random(settings.seed)
        self.fake = Faker("id_ID")
        self.fake.seed_instance(settings.seed)
        self._last_realtime_requested_at: datetime | None = None

    def seed_reference_and_master(
        self,
        customer_count: int | None = None,
        driver_count: int | None = None,
        first_business_date: Any | None = None,
    ) -> None:
        """Seed master data before the first possible ride date.

        This avoids customers or drivers appearing to be created after historical rides
        that already reference them.
        """
        customer_count = (
            self.settings.customer_count if customer_count is None else customer_count
        )
        driver_count = (
            self.settings.driver_count if driver_count is None else driver_count
        )
        if customer_count <= 0 or driver_count <= 0:
            raise ValueError("customer_count and driver_count must be positive")

        if first_business_date is None:
            first_business_date = jakarta_now_naive().date() - timedelta(
                days=self.settings.bootstrap_days
            )
        master_cutoff_local = datetime.combine(first_business_date, datetime.min.time())
        master_cutoff_utc = utc_naive_from_jakarta(master_cutoff_local)

        with connect(self.settings) as conn:
            self._seed_cities_and_zones(conn, master_cutoff_utc)
            self._seed_customers(conn, customer_count, master_cutoff_utc)
            self._seed_drivers_and_vehicles(conn, driver_count, master_cutoff_utc)
        LOGGER.info("Reference and master data are ready before %s", master_cutoff_utc)

    def _seed_cities_and_zones(self, conn: Connection, cutoff_utc: datetime) -> None:
        reference_created_at = cutoff_utc - timedelta(days=365 * 3)
        with conn.cursor() as cur:
            for city in CITIES:
                cur.execute(
                    """
                    INSERT INTO cities (
                        city_code, city_name, timezone, is_active, created_at, updated_at
                    )
                    VALUES (%s, %s, 'Asia/Jakarta', TRUE, %s, %s)
                    ON CONFLICT (city_code) DO UPDATE SET
                        city_name = EXCLUDED.city_name,
                        timezone = EXCLUDED.timezone,
                        is_active = TRUE
                    WHERE cities.city_name IS DISTINCT FROM EXCLUDED.city_name
                       OR cities.timezone IS DISTINCT FROM EXCLUDED.timezone
                       OR cities.is_active IS DISTINCT FROM TRUE
                    """,
                    (
                        city["city_code"],
                        city["city_name"],
                        reference_created_at,
                        reference_created_at,
                    ),
                )
            cur.execute("SELECT city_id, city_code FROM cities")
            city_ids = {code: city_id for city_id, code in cur.fetchall()}
            for city_code, zones in ZONES.items():
                for zone_code, zone_name, zone_type, is_hotspot in zones:
                    cur.execute(
                        """
                        INSERT INTO zones (
                            city_id, zone_code, zone_name, zone_type, is_hotspot,
                            is_active, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s)
                        ON CONFLICT (zone_code) DO UPDATE SET
                            city_id = EXCLUDED.city_id,
                            zone_name = EXCLUDED.zone_name,
                            zone_type = EXCLUDED.zone_type,
                            is_hotspot = EXCLUDED.is_hotspot,
                            is_active = TRUE
                        WHERE zones.city_id IS DISTINCT FROM EXCLUDED.city_id
                           OR zones.zone_name IS DISTINCT FROM EXCLUDED.zone_name
                           OR zones.zone_type IS DISTINCT FROM EXCLUDED.zone_type
                           OR zones.is_hotspot IS DISTINCT FROM EXCLUDED.is_hotspot
                           OR zones.is_active IS DISTINCT FROM TRUE
                        """,
                        (
                            city_ids[city_code],
                            zone_code,
                            zone_name,
                            zone_type,
                            is_hotspot,
                            reference_created_at,
                            reference_created_at,
                        ),
                    )

    def _random_datetime_between(self, start: datetime, end: datetime) -> datetime:
        if end <= start:
            return start
        seconds = int((end - start).total_seconds())
        return start + timedelta(seconds=self.rng.randint(0, seconds))

    def _seed_customers(
        self,
        conn: Connection,
        target_count: int,
        cutoff_utc: datetime,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            existing = cur.fetchone()[0]
            missing = max(target_count - existing, 0)
            if not missing:
                return
            cur.execute("SELECT city_id, city_code FROM cities WHERE is_active")
            city_id_by_code = {code: city_id for city_id, code in cur.fetchall()}
            rows = []
            registration_start = cutoff_utc - timedelta(days=730)
            registration_end = cutoff_utc - timedelta(minutes=5)
            for _ in range(missing):
                city_code = self.rng.choices(CITY_CODES, weights=CITY_WEIGHTS, k=1)[0]
                created_at = self._random_datetime_between(
                    registration_start,
                    registration_end,
                )
                rows.append(
                    (
                        self.fake.name(),
                        city_id_by_code[city_code],
                        created_at,
                        created_at,
                    )
                )
            cur.executemany(
                """
                INSERT INTO customers (
                    customer_name, registered_city_id, customer_status,
                    created_at, updated_at
                ) VALUES (%s, %s, 'ACTIVE', %s, %s)
                """,
                rows,
            )
            LOGGER.info("Inserted %s customers", missing)

    def _seed_drivers_and_vehicles(
        self,
        conn: Connection,
        target_count: int,
        cutoff_utc: datetime,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM drivers")
            existing = cur.fetchone()[0]
            missing = max(target_count - existing, 0)
            if not missing:
                return
            cur.execute("SELECT city_id, city_code FROM cities WHERE is_active")
            city_id_by_code = {code: city_id for city_id, code in cur.fetchall()}
            driver_start = cutoff_utc - timedelta(days=1095)
            driver_end = cutoff_utc - timedelta(days=30)
            for _ in range(missing):
                city_code = self.rng.choices(CITY_CODES, weights=CITY_WEIGHTS, k=1)[0]
                service_type = "BIKE" if self.rng.random() < 0.68 else "CAR"
                rating = round(self.rng.uniform(4.25, 5.0), 2)
                created_at = self._random_datetime_between(driver_start, driver_end)
                cur.execute(
                    """
                    INSERT INTO drivers (
                        driver_name, city_id, service_type, driver_status, rating,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, 'AVAILABLE', %s, %s, %s)
                    RETURNING driver_id
                    """,
                    (
                        self.fake.name(),
                        city_id_by_code[city_code],
                        service_type,
                        rating,
                        created_at,
                        created_at,
                    ),
                )
                driver_id = cur.fetchone()[0]
                vehicle_type = "MOTORCYCLE" if service_type == "BIKE" else "CAR"
                vehicle_created_at = min(
                    cutoff_utc - timedelta(minutes=1),
                    created_at + timedelta(days=self.rng.randint(0, 14)),
                )
                cur.execute(
                    """
                    INSERT INTO vehicles (
                        driver_id, vehicle_type, vehicle_year, vehicle_status,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, 'ACTIVE', %s, %s)
                    """,
                    (
                        driver_id,
                        vehicle_type,
                        self.rng.randint(2017, 2026),
                        vehicle_created_at,
                        vehicle_created_at,
                    ),
                )
            LOGGER.info("Inserted %s drivers and vehicles", missing)

    def bootstrap(
        self,
        days: int,
        rides_per_day: int,
        customer_count: int | None = None,
        driver_count: int | None = None,
    ) -> None:
        if days <= 0 or rides_per_day <= 0:
            raise ValueError("days and rides_per_day must be positive")
        maximum_daily_volume = int(rides_per_day * 1.10 * 1.20)
        if maximum_daily_volume >= 86_400:
            raise ValueError(
                "rides_per_day is too large: weekend and payday multipliers can "
                "exceed the 86,400 unique seconds available in one day"
            )

        today = jakarta_now_naive().date()
        first_ride_date = today - timedelta(days=days)
        self.seed_reference_and_master(
            customer_count=customer_count,
            driver_count=driver_count,
            first_business_date=first_ride_date,
        )

        with connect(self.settings) as conn:
            dimensions = self._load_dimensions(conn)
            for days_ago in range(days, 0, -1):
                ride_date = today - timedelta(days=days_ago)
                daily_volume = rides_per_day
                if ride_date.weekday() >= 5:
                    daily_volume = int(daily_volume * 1.10)
                if ride_date.day in {25, 26, 27, 28}:
                    daily_volume = int(daily_volume * 1.20)

                requested_times = self._historical_requested_times(
                    ride_date,
                    daily_volume,
                )
                for requested_at in requested_times:
                    self._insert_historical_ride(conn, requested_at, dimensions)
                conn.commit()
                LOGGER.info("Bootstrapped %s rides for %s", daily_volume, ride_date)

    def _historical_requested_times(
        self,
        ride_date: Any,
        count: int,
    ) -> list[datetime]:
        """Generate unique, sorted Jakarta request timestamps across a day.

        The hourly distribution remains seasonal, but timestamps are no longer
        produced from one common wall-clock value.
        """
        hourly_weights = [
            1, 1, 1, 1, 1, 2,
            7, 10, 11, 8, 5, 7,
            8, 6, 5, 7, 10, 12,
            13, 12, 9, 6, 4, 3,
        ]
        if ride_date.weekday() >= 5:
            hourly_weights = [
                1, 1, 1, 1, 1, 1,
                2, 3, 4, 5, 6, 8,
                9, 9, 8, 8, 9, 11,
                12, 12, 10, 8, 6, 4,
            ]

        used_seconds: set[int] = set()
        while len(used_seconds) < count:
            hour = self.rng.choices(range(24), weights=hourly_weights, k=1)[0]
            second_of_day = hour * 3600 + self.rng.randint(0, 3599)
            used_seconds.add(second_of_day)

        midnight = datetime.combine(ride_date, datetime.min.time())
        return [
            midnight + timedelta(seconds=second)
            for second in sorted(used_seconds)
        ]

    def _stable_rng(self, entity_id: int, salt: int) -> random.Random:
        return random.Random(
            self.settings.seed * 1_000_003 + entity_id * 97_409 + salt
        )

    def _accept_delay_seconds(self, entity_id: int) -> int:
        return self._stable_rng(entity_id, 11).randint(8, 90)

    def _search_timeout_seconds(self, entity_id: int) -> int:
        return self._stable_rng(entity_id, 13).randint(90, 300)

    def _arrival_delay_minutes(self, entity_id: int, service_type: str) -> float:
        rng = self._stable_rng(entity_id, 17)
        low, high = (2.5, 13.0) if service_type == "BIKE" else (4.0, 18.0)
        return round(rng.uniform(low, high), 2)

    def _pickup_wait_minutes(self, entity_id: int) -> float:
        return round(self._stable_rng(entity_id, 19).uniform(0.75, 4.5), 2)

    def _actual_duration_minutes(self, entity_id: int, estimated: float) -> float:
        factor = self._stable_rng(entity_id, 23).uniform(0.90, 1.25)
        return round(max(5.0, estimated * factor), 2)

    def _payment_processing_seconds(self, entity_id: int) -> int:
        return self._stable_rng(entity_id, 29).randint(10, 180)

    def _load_dimensions(self, conn: Connection) -> dict[str, Any]:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT city_id, city_code FROM cities WHERE is_active")
            cities = cur.fetchall()
            cur.execute(
                """
                SELECT z.zone_id, z.zone_code, z.zone_name, z.zone_type, z.is_hotspot,
                       c.city_id, c.city_code
                FROM zones z
                JOIN cities c ON c.city_id = z.city_id
                WHERE z.is_active AND c.is_active
                """
            )
            zones = cur.fetchall()
            cur.execute("SELECT customer_id FROM customers WHERE customer_status = 'ACTIVE'")
            customers = [row["customer_id"] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT driver_id, city_id, service_type
                FROM drivers
                WHERE driver_status IN ('AVAILABLE', 'ON_TRIP')
                """
            )
            drivers = cur.fetchall()
        return {"cities": cities, "zones": zones, "customers": customers, "drivers": drivers}

    def _select_context(
        self,
        conn: Connection,
        requested_at: datetime,
        dimensions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        dimensions = dimensions or self._load_dimensions(conn)
        city_code = self.rng.choices(CITY_CODES, weights=CITY_WEIGHTS, k=1)[0]
        city = next(city for city in dimensions["cities"] if city["city_code"] == city_code)
        city_zones = [zone for zone in dimensions["zones"] if zone["city_code"] == city_code]
        pickup_weights = [2.5 if zone["is_hotspot"] else 1.0 for zone in city_zones]
        pickup = self.rng.choices(city_zones, weights=pickup_weights, k=1)[0]
        drop_candidates = [zone for zone in city_zones if zone["zone_id"] != pickup["zone_id"]]
        dropoff = self.rng.choice(drop_candidates)
        service_type = choose_service_type(self.rng, requested_at, pickup["zone_type"])
        distance_km = round(self.rng.uniform(1.5, 18.0 if service_type == "CAR" else 12.0), 2)
        duration_min = duration_from_distance(distance_km, service_type, self.rng)
        surge = surge_multiplier(requested_at, city_code, pickup["is_hotspot"], self.rng)
        fare = calculate_fare(service_type, distance_km, duration_min, surge, self.rng)
        eligible_drivers = [
            driver for driver in dimensions["drivers"]
            if driver["city_id"] == city["city_id"] and driver["service_type"] == service_type
        ]
        return {
            "city": city,
            "pickup": pickup,
            "dropoff": dropoff,
            "service_type": service_type,
            "distance_km": distance_km,
            "duration_min": duration_min,
            "fare": fare,
            "customer_id": self.rng.choice(dimensions["customers"]),
            "eligible_drivers": eligible_drivers,
        }

    def _insert_historical_ride(
        self,
        conn: Connection,
        requested_at: datetime,
        dimensions: dict[str, Any],
    ) -> int:
        context = self._select_context(conn, requested_at, dimensions)
        city = context["city"]
        pickup = context["pickup"]
        fare = context["fare"]

        accepted = self.rng.random() < acceptance_probability(
            requested_at,
            city["city_code"],
            pickup["is_hotspot"],
        )
        driver = (
            self.rng.choice(context["eligible_drivers"])
            if accepted and context["eligible_drivers"]
            else None
        )

        status = "NO_DRIVER"
        accepted_at = arrived_at = started_at = completed_at = cancelled_at = None
        cancelled_by = cancellation_reason = None
        actual_distance = actual_duration = None
        status_version = 2

        if accepted and driver:
            accepted_at = requested_at + timedelta(
                seconds=self.rng.randint(8, 90)
            )
            arrival_delay = self.rng.uniform(
                2.5 if context["service_type"] == "BIKE" else 4.0,
                13.0 if context["service_type"] == "BIKE" else 18.0,
            )
            if requested_at.weekday() < 5 and (
                6 <= requested_at.hour <= 9 or 16 <= requested_at.hour <= 20
            ):
                arrival_delay *= self.rng.uniform(1.05, 1.35)
            planned_arrived_at = accepted_at + timedelta(minutes=arrival_delay)
            cancel_probability = cancellation_probability(
                requested_at,
                arrival_delay,
                float(fare.surge_multiplier),
            )

            if self.rng.random() < cancel_probability:
                status = "CANCELLED"
                if self.rng.random() < 0.75:
                    cancelled_at = accepted_at + timedelta(
                        seconds=self.rng.randint(
                            30,
                            max(31, int(arrival_delay * 60 * 0.85)),
                        )
                    )
                else:
                    arrived_at = planned_arrived_at
                    cancelled_at = arrived_at + timedelta(
                        seconds=self.rng.randint(30, 240)
                    )
                cancelled_by = self.rng.choice(["CUSTOMER", "DRIVER"])
                cancellation_reason = self.rng.choice(
                    [
                        "LONG_WAIT_TIME",
                        "CUSTOMER_CHANGED_MIND",
                        "DRIVER_REQUEST",
                        "PICKUP_ISSUE",
                    ]
                )
                status_version = 3 if arrived_at is None else 4
            else:
                status = "COMPLETED"
                arrived_at = planned_arrived_at
                started_at = arrived_at + timedelta(
                    minutes=self.rng.uniform(0.75, 4.5)
                )
                actual_duration = round(
                    context["duration_min"] * self.rng.uniform(0.90, 1.25),
                    2,
                )
                actual_distance = round(
                    context["distance_km"] * self.rng.uniform(0.95, 1.12),
                    2,
                )
                completed_at = started_at + timedelta(minutes=actual_duration)
                status_version = 5
        else:
            cancelled_at = requested_at + timedelta(
                seconds=self.rng.randint(90, 300)
            )
            cancelled_by = "SYSTEM"
            cancellation_reason = "NO_DRIVER_AVAILABLE"

        final_update = completed_at or cancelled_at or requested_at
        stored_requested_at = utc_naive_from_jakarta(requested_at)
        stored_accepted_at = utc_naive_from_jakarta(accepted_at) if accepted_at else None
        stored_arrived_at = utc_naive_from_jakarta(arrived_at) if arrived_at else None
        stored_started_at = utc_naive_from_jakarta(started_at) if started_at else None
        stored_completed_at = utc_naive_from_jakarta(completed_at) if completed_at else None
        stored_cancelled_at = utc_naive_from_jakarta(cancelled_at) if cancelled_at else None
        stored_final_update = utc_naive_from_jakarta(final_update)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rides (
                    customer_id, driver_id, city_id, service_type,
                    pickup_zone_id, dropoff_zone_id, ride_status,
                    requested_at, accepted_at, driver_arrived_at, started_at,
                    completed_at, cancelled_at, cancelled_by, cancellation_reason,
                    estimated_distance_km, actual_distance_km,
                    estimated_duration_min, actual_duration_min,
                    base_fare, distance_fare, time_fare, surge_multiplier,
                    gross_fare, discount_amount, final_fare,
                    status_version, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                ) RETURNING ride_id
                """,
                (
                    context["customer_id"],
                    driver["driver_id"] if driver else None,
                    city["city_id"],
                    context["service_type"],
                    pickup["zone_id"],
                    context["dropoff"]["zone_id"],
                    status,
                    stored_requested_at,
                    stored_accepted_at,
                    stored_arrived_at,
                    stored_started_at,
                    stored_completed_at,
                    stored_cancelled_at,
                    cancelled_by,
                    cancellation_reason,
                    context["distance_km"],
                    actual_distance,
                    context["duration_min"],
                    actual_duration,
                    fare.base_fare,
                    fare.distance_fare,
                    fare.time_fare,
                    fare.surge_multiplier,
                    fare.gross_fare,
                    fare.discount_amount,
                    fare.final_fare,
                    status_version,
                    stored_requested_at,
                    stored_final_update,
                ),
            )
            ride_id = cur.fetchone()[0]
            if status == "COMPLETED":
                self._insert_historical_payment(
                    cur,
                    ride_id,
                    fare.final_fare,
                    completed_at,
                    city["city_code"],
                )
        return ride_id

    def _payment_method(self) -> str:
        return self.rng.choices(["CASH", "EWALLET", "CARD"], weights=[0.32, 0.50, 0.18], k=1)[0]

    def _payment_failure_probability(self, method: str, city_code: str, moment: datetime) -> float:
        probability = {"CASH": 0.01, "EWALLET": 0.05, "CARD": 0.06}[method]
        if city_code == "SBY" and 19 <= moment.hour <= 21 and method != "CASH":
            probability += 0.10
        return probability

    def _insert_historical_payment(
        self,
        cur: Any,
        ride_id: int,
        amount: Decimal,
        completed_at: datetime,
        city_code: str,
    ) -> None:
        method = self._payment_method()
        created_at = completed_at + timedelta(seconds=self.rng.randint(5, 90))
        failed = self.rng.random() < self._payment_failure_probability(
            method,
            city_code,
            completed_at,
        )
        status = "FAILED" if failed else "PAID"
        failure_reason = None
        paid_at = None

        if failed:
            failure_reason = self.rng.choice(
                [
                    "GATEWAY_TIMEOUT",
                    "EWALLET_PROVIDER_DOWN",
                    "CARD_DECLINED",
                    "INSUFFICIENT_BALANCE",
                ]
            )
            updated_at = created_at + timedelta(seconds=self.rng.randint(15, 180))
        else:
            paid_at = created_at + timedelta(
                seconds=self.rng.randint(2, 120 if method != "CASH" else 30)
            )
            updated_at = paid_at

        platform_fee = (amount * Decimal("0.20")).quantize(Decimal("0.01"))
        driver_earning = amount - platform_fee
        stored_created_at = utc_naive_from_jakarta(created_at)
        stored_paid_at = utc_naive_from_jakarta(paid_at) if paid_at else None
        stored_updated_at = utc_naive_from_jakarta(updated_at)
        cur.execute(
            """
            INSERT INTO payments (
                ride_id, payment_method, payment_status, payment_amount,
                platform_fee, driver_earning, failure_reason, paid_at,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ride_id) DO NOTHING
            """,
            (
                ride_id,
                method,
                status,
                amount,
                platform_fee,
                driver_earning,
                failure_reason,
                stored_paid_at,
                stored_created_at,
                stored_updated_at,
            ),
        )

    def create_realtime_rides(self, count: int) -> list[int]:
        now_utc = utc_now_naive()
        now_local = jakarta_naive_from_utc(now_utc)
        volume_multiplier = 1.0
        if now_local.weekday() < 5 and (
            6 <= now_local.hour <= 9 or 16 <= now_local.hour <= 20
        ):
            volume_multiplier *= 1.50
        elif 0 <= now_local.hour <= 5:
            volume_multiplier *= 0.40
        if now_local.weekday() >= 5:
            volume_multiplier *= 1.10
        if now_local.day in {25, 26, 27, 28}:
            volume_multiplier *= 1.20
        count = max(1, round(count * volume_multiplier))

        interval_start = self._last_realtime_requested_at or (
            now_utc - timedelta(seconds=max(self.settings.interval_seconds, 1))
        )
        available_seconds = max((now_utc - interval_start).total_seconds(), 1.0)
        requested_times = sorted(
            interval_start
            + timedelta(seconds=self.rng.uniform(0.0, available_seconds))
            for _ in range(count)
        )
        self._last_realtime_requested_at = now_utc

        ride_ids: list[int] = []
        with connect(self.settings) as conn:
            dimensions = self._load_dimensions(conn)
            for requested_at_utc in requested_times:
                requested_at_local = jakarta_naive_from_utc(requested_at_utc)
                context = self._select_context(conn, requested_at_local, dimensions)
                fare = context["fare"]
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO rides (
                            customer_id, city_id, service_type,
                            pickup_zone_id, dropoff_zone_id,
                            ride_status, requested_at,
                            estimated_distance_km, estimated_duration_min,
                            base_fare, distance_fare, time_fare, surge_multiplier,
                            gross_fare, discount_amount, final_fare,
                            status_version, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            'REQUESTED', %s,
                            %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            1, %s, %s
                        ) RETURNING ride_id
                        """,
                        (
                            context["customer_id"],
                            context["city"]["city_id"],
                            context["service_type"],
                            context["pickup"]["zone_id"],
                            context["dropoff"]["zone_id"],
                            requested_at_utc,
                            context["distance_km"],
                            context["duration_min"],
                            fare.base_fare,
                            fare.distance_fare,
                            fare.time_fare,
                            fare.surge_multiplier,
                            fare.gross_fare,
                            fare.discount_amount,
                            fare.final_fare,
                            requested_at_utc,
                            requested_at_utc,
                        ),
                    )
                    ride_ids.append(cur.fetchone()[0])
        LOGGER.info("Created realtime rides: %s", ride_ids)
        return ride_ids

    def progress_realtime(self) -> dict[str, int]:
        counters = {
            "accepted": 0,
            "no_driver": 0,
            "arrived": 0,
            "cancelled": 0,
            "started": 0,
            "completed": 0,
            "settled": 0,
        }
        now = utc_now_naive()

        with connect(self.settings) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT r.*, c.city_code, z.is_hotspot
                    FROM rides r
                    JOIN cities c ON c.city_id = r.city_id
                    JOIN zones z ON z.zone_id = r.pickup_zone_id
                    WHERE r.ride_status IN (
                        'REQUESTED', 'ACCEPTED', 'DRIVER_ARRIVED', 'IN_PROGRESS'
                    )
                      AND r.requested_at >= %s
                    ORDER BY r.requested_at
                    LIMIT 500
                    """,
                    (now - timedelta(days=2),),
                )

                for ride in cur.fetchall():
                    ride_id = int(ride["ride_id"])
                    requested_local = jakarta_naive_from_utc(ride["requested_at"])

                    if ride["ride_status"] == "REQUESTED":
                        acceptance_due = ride["requested_at"] + timedelta(
                            seconds=self._accept_delay_seconds(ride_id)
                        )
                        no_driver_due = ride["requested_at"] + timedelta(
                            seconds=self._search_timeout_seconds(ride_id)
                        )
                        probability = acceptance_probability(
                            requested_local,
                            ride["city_code"],
                            ride["is_hotspot"],
                        )
                        accepted_decision = (
                            self._stable_rng(ride_id, 31).random() <= probability
                        )

                        if accepted_decision and now >= acceptance_due:
                            cur.execute(
                                """
                                SELECT driver_id
                                FROM drivers
                                WHERE city_id = %s
                                  AND service_type = %s
                                  AND driver_status = 'AVAILABLE'
                                ORDER BY random()
                                LIMIT 1
                                """,
                                (ride["city_id"], ride["service_type"]),
                            )
                            driver_row = cur.fetchone()
                            if driver_row:
                                cur.execute(
                                    """
                                    UPDATE rides
                                    SET driver_id=%s,
                                        ride_status='ACCEPTED',
                                        accepted_at=%s,
                                        updated_at=%s,
                                        status_version=status_version+1
                                    WHERE ride_id=%s
                                    """,
                                    (
                                        driver_row["driver_id"],
                                        acceptance_due,
                                        acceptance_due,
                                        ride_id,
                                    ),
                                )
                                cur.execute(
                                    """
                                    UPDATE drivers
                                    SET driver_status='ON_TRIP', updated_at=%s
                                    WHERE driver_id=%s
                                    """,
                                    (acceptance_due, driver_row["driver_id"]),
                                )
                                counters["accepted"] += 1
                                continue

                        if now >= no_driver_due:
                            cur.execute(
                                """
                                UPDATE rides
                                SET ride_status='NO_DRIVER',
                                    cancelled_at=%s,
                                    cancelled_by='SYSTEM',
                                    cancellation_reason='NO_DRIVER_AVAILABLE',
                                    updated_at=%s,
                                    status_version=status_version+1
                                WHERE ride_id=%s
                                """,
                                (no_driver_due, no_driver_due, ride_id),
                            )
                            counters["no_driver"] += 1

                    elif ride["ride_status"] == "ACCEPTED":
                        arrival_delay = self._arrival_delay_minutes(
                            ride_id,
                            ride["service_type"],
                        )
                        arrived_due = ride["accepted_at"] + timedelta(
                            minutes=arrival_delay
                        )
                        cancel_probability = cancellation_probability(
                            requested_local,
                            arrival_delay,
                            float(ride["surge_multiplier"]),
                        )
                        cancelled_decision = (
                            self._stable_rng(ride_id, 37).random()
                            < cancel_probability
                        )

                        if cancelled_decision:
                            cancel_fraction = self._stable_rng(ride_id, 41).uniform(
                                0.20,
                                0.85,
                            )
                            cancelled_due = ride["accepted_at"] + timedelta(
                                minutes=arrival_delay * cancel_fraction
                            )
                            if now >= cancelled_due:
                                cur.execute(
                                    """
                                    UPDATE rides
                                    SET ride_status='CANCELLED',
                                        cancelled_at=%s,
                                        cancelled_by=%s,
                                        cancellation_reason=%s,
                                        updated_at=%s,
                                        status_version=status_version+1
                                    WHERE ride_id=%s
                                    """,
                                    (
                                        cancelled_due,
                                        self._stable_rng(ride_id, 43).choice(
                                            ["CUSTOMER", "DRIVER"]
                                        ),
                                        self._stable_rng(ride_id, 47).choice(
                                            [
                                                "LONG_WAIT_TIME",
                                                "CUSTOMER_CHANGED_MIND",
                                                "DRIVER_REQUEST",
                                            ]
                                        ),
                                        cancelled_due,
                                        ride_id,
                                    ),
                                )
                                cur.execute(
                                    """
                                    UPDATE drivers
                                    SET driver_status='AVAILABLE', updated_at=%s
                                    WHERE driver_id=%s
                                    """,
                                    (cancelled_due, ride["driver_id"]),
                                )
                                counters["cancelled"] += 1
                        elif now >= arrived_due:
                            cur.execute(
                                """
                                UPDATE rides
                                SET ride_status='DRIVER_ARRIVED',
                                    driver_arrived_at=%s,
                                    updated_at=%s,
                                    status_version=status_version+1
                                WHERE ride_id=%s
                                """,
                                (arrived_due, arrived_due, ride_id),
                            )
                            counters["arrived"] += 1

                    elif ride["ride_status"] == "DRIVER_ARRIVED":
                        started_due = ride["driver_arrived_at"] + timedelta(
                            minutes=self._pickup_wait_minutes(ride_id)
                        )
                        if now >= started_due:
                            cur.execute(
                                """
                                UPDATE rides
                                SET ride_status='IN_PROGRESS',
                                    started_at=%s,
                                    updated_at=%s,
                                    status_version=status_version+1
                                WHERE ride_id=%s
                                """,
                                (started_due, started_due, ride_id),
                            )
                            counters["started"] += 1

                    elif ride["ride_status"] == "IN_PROGRESS":
                        actual_duration = self._actual_duration_minutes(
                            ride_id,
                            float(ride["estimated_duration_min"]),
                        )
                        completed_due = ride["started_at"] + timedelta(
                            minutes=actual_duration
                        )
                        if now >= completed_due:
                            actual_distance = round(
                                float(ride["estimated_distance_km"])
                                * self._stable_rng(ride_id, 53).uniform(0.95, 1.12),
                                2,
                            )
                            cur.execute(
                                """
                                UPDATE rides
                                SET ride_status='COMPLETED',
                                    completed_at=%s,
                                    actual_distance_km=%s,
                                    actual_duration_min=%s,
                                    updated_at=%s,
                                    status_version=status_version+1
                                WHERE ride_id=%s
                                """,
                                (
                                    completed_due,
                                    actual_distance,
                                    actual_duration,
                                    completed_due,
                                    ride_id,
                                ),
                            )
                            cur.execute(
                                """
                                UPDATE drivers
                                SET driver_status='AVAILABLE', updated_at=%s
                                WHERE driver_id=%s
                                """,
                                (completed_due, ride["driver_id"]),
                            )
                            platform_fee = (
                                ride["final_fare"] * Decimal("0.20")
                            ).quantize(Decimal("0.01"))
                            payment_created_at = completed_due + timedelta(
                                seconds=self._stable_rng(ride_id, 59).randint(5, 60)
                            )
                            cur.execute(
                                """
                                INSERT INTO payments (
                                    ride_id, payment_method, payment_status,
                                    payment_amount, platform_fee, driver_earning,
                                    created_at, updated_at
                                ) VALUES (%s, %s, 'PENDING', %s, %s, %s, %s, %s)
                                ON CONFLICT (ride_id) DO NOTHING
                                """,
                                (
                                    ride_id,
                                    self._stable_rng(ride_id, 61).choices(
                                        ["CASH", "EWALLET", "CARD"],
                                        weights=[0.32, 0.50, 0.18],
                                        k=1,
                                    )[0],
                                    ride["final_fare"],
                                    platform_fee,
                                    ride["final_fare"] - platform_fee,
                                    payment_created_at,
                                    payment_created_at,
                                ),
                            )
                            counters["completed"] += 1

                cur.execute(
                    """
                    SELECT p.*, c.city_code, r.completed_at
                    FROM payments p
                    JOIN rides r ON r.ride_id = p.ride_id
                    JOIN cities c ON c.city_id = r.city_id
                    WHERE p.payment_status = 'PENDING'
                    ORDER BY p.created_at
                    LIMIT 500
                    """
                )
                for payment in cur.fetchall():
                    payment_id = int(payment["payment_id"])
                    settle_due = payment["created_at"] + timedelta(
                        seconds=self._payment_processing_seconds(payment_id)
                    )
                    if now < settle_due:
                        continue

                    failure_probability = self._payment_failure_probability(
                        payment["payment_method"],
                        payment["city_code"],
                        jakarta_naive_from_utc(payment["completed_at"]),
                    )
                    failed = (
                        self._stable_rng(payment_id, 67).random()
                        < failure_probability
                    )
                    if failed:
                        cur.execute(
                            """
                            UPDATE payments
                            SET payment_status='FAILED',
                                failure_reason=%s,
                                updated_at=%s
                            WHERE payment_id=%s
                            """,
                            (
                                self._stable_rng(payment_id, 71).choice(
                                    [
                                        "GATEWAY_TIMEOUT",
                                        "EWALLET_PROVIDER_DOWN",
                                        "CARD_DECLINED",
                                        "INSUFFICIENT_BALANCE",
                                    ]
                                ),
                                settle_due,
                                payment_id,
                            ),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE payments
                            SET payment_status='PAID',
                                paid_at=%s,
                                failure_reason=NULL,
                                updated_at=%s
                            WHERE payment_id=%s
                            """,
                            (settle_due, settle_due, payment_id),
                        )
                    counters["settled"] += 1
        return counters

    def realtime_loop(self) -> None:
        self.seed_reference_and_master()
        LOGGER.info(
            "Realtime generator started: %s rides every %s seconds",
            self.settings.rides_per_tick,
            self.settings.interval_seconds,
        )
        while True:
            try:
                self.create_realtime_rides(self.settings.rides_per_tick)
                counters = self.progress_realtime()
                LOGGER.info("Lifecycle updates: %s", counters)
            except Exception:
                LOGGER.exception("Realtime generator tick failed")
            time.sleep(self.settings.interval_seconds)

    def crud_demo(self) -> None:
        self.seed_reference_and_master()
        with connect(self.settings) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT city_id FROM cities WHERE city_code='JKT'")
                city_id = cur.fetchone()["city_id"]
                cur.execute(
                    """
                    INSERT INTO customers (customer_name, registered_city_id, customer_status)
                    VALUES ('CRUD Demo Customer', %s, 'ACTIVE') RETURNING *
                    """,
                    (city_id,),
                )
                created = cur.fetchone()
                LOGGER.info("CREATE: %s", created)
                cur.execute("SELECT * FROM customers WHERE customer_id=%s", (created["customer_id"],))
                LOGGER.info("READ: %s", cur.fetchone())
                cur.execute(
                    """
                    UPDATE customers SET customer_name='CRUD Demo Updated', customer_status='INACTIVE'
                    WHERE customer_id=%s RETURNING *
                    """,
                    (created["customer_id"],),
                )
                LOGGER.info("UPDATE: %s", cur.fetchone())
                cur.execute("DELETE FROM customers WHERE customer_id=%s RETURNING customer_id", (created["customer_id"],))
                LOGGER.info("DELETE customer_id=%s", cur.fetchone()["customer_id"])
