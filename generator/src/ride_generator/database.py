from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg import Connection

from .config import Settings


@contextmanager
def connect(settings: Settings, retries: int = 30) -> Iterator[Connection]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            connection = psycopg.connect(settings.dsn, autocommit=False)
            try:
                yield connection
                connection.commit()
                return
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        except psycopg.OperationalError as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(min(attempt, 5))
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_error}")
