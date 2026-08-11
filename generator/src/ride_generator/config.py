from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    pg_host: str = os.getenv("PGHOST", "localhost")
    pg_port: int = int(os.getenv("PGPORT", "5432"))
    pg_database: str = os.getenv("PGDATABASE", "ride_hailing")
    pg_user: str = os.getenv("PGUSER", "ride_user")
    pg_password: str = os.getenv("PGPASSWORD", "ride_password")
    seed: int = int(os.getenv("GENERATOR_SEED", "42"))
    interval_seconds: int = int(os.getenv("GENERATOR_INTERVAL_SECONDS", "15"))
    rides_per_tick: int = int(os.getenv("GENERATOR_RIDES_PER_TICK", "5"))
    bootstrap_days: int = int(os.getenv("GENERATOR_BOOTSTRAP_DAYS", "90"))
    bootstrap_rides_per_day: int = int(os.getenv("GENERATOR_BOOTSTRAP_RIDES_PER_DAY", "2000"))
    customer_count: int = int(os.getenv("GENERATOR_CUSTOMER_COUNT", "20000"))
    driver_count: int = int(os.getenv("GENERATOR_DRIVER_COUNT", "2000"))

    @property
    def dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password}"
        )
