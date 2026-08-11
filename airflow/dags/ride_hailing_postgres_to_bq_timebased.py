"""
DAG: ride_hailing_postgres_to_bq_timebased
Airflow: 3.x
Engine: Trino federation between PostgreSQL and BigQuery.

The DAG uses a half-open data interval [window_start, window_end) and a
per-table lookback. Manual window parameters are interpreted in Asia/Jakarta
when no timezone offset is supplied, then normalized to UTC because the source
PostgreSQL schema stores naive UTC timestamps.

Current MVP strategy
--------------------
* cities, zones: full extraction + current-state upsert.
* customers, drivers, vehicles: updated_at incremental upsert, 2-day lookback.
* rides, payments: updated_at incremental upsert, 7-day lookback.

No active append-only table exists in the current seven-table MVP schema.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from helpers.refactored_trino_helper import TableConfig, make_table_task_group


# =============================================================================
# Global configuration
# =============================================================================

DAG_ID = "ride_hailing_postgres_to_bq_timebased"
BUSINESS_TZ = "Asia/Jakarta"
SOURCE_TIMESTAMP_TZ = "UTC"

TRINO_CONN_ID = os.getenv("AIRFLOW_TRINO_CONN_ID", "trino_default")
GCP_CONN_ID = os.getenv("AIRFLOW_GCP_CONN_ID", "google_cloud_default")
TRINO_BQ_CAT = os.getenv("TRINO_CATALOG_BIGQUERY", "bigquery")
TRINO_PG_CAT = os.getenv("TRINO_CATALOG_POSTGRES", "postgresql")

BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "taxi-pipeline-484508")
BQ_DATASET = os.getenv("BQ_RAW_DATASET", "raw_ride_hailing")
BQ_LOCATION = os.getenv("GCP_LOCATION", "US")
PG_SCHEMA = os.getenv("POSTGRES_SOURCE_SCHEMA", "public")

BASE_LABELS = {
    "env": os.getenv("ENVIRONMENT", "dev"),
    "team": "data-eng",
    "layer": "raw",
    "pipeline": "timebased-ingestion",
    "dag-id": DAG_ID,
}

SHARED = {
    "bq_project": BQ_PROJECT,
    "bq_dataset": BQ_DATASET,
    "bq_location": BQ_LOCATION,
    "pg_schema": PG_SCHEMA,
    "trino_conn_id": TRINO_CONN_ID,
    "gcp_conn_id": GCP_CONN_ID,
    "trino_bq_cat": TRINO_BQ_CAT,
    "trino_pg_cat": TRINO_PG_CAT,
    "source_timestamp_timezone": SOURCE_TIMESTAMP_TZ,
    "window_task_id": "build_window",
    "dag_labels": BASE_LABELS,
}


# =============================================================================
# Source table configurations
# =============================================================================

TABLE_CONFIGS = [
    # -------------------------------------------------------------------------
    # Small reference tables: full source scan is intentional.
    # Physical deactivation is represented by is_active, not hard delete.
    # -------------------------------------------------------------------------
    TableConfig(
        pg_table="cities",
        bq_final_table="raw_cities",
        merge_key="city_id",
        watermark_field="updated_at",
        extraction_mode="full",
        lookback_days=0,
        source_order_fields=["updated_at"],
        cluster_fields=["city_code", "is_active"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "city_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "city_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "city_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "timezone", "type": "STRING", "mode": "REQUIRED"},
            {"name": "is_active", "type": "BOOL", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            city_id, city_code, city_name, timezone, is_active,
            created_at, updated_at
        """,
    ),
    TableConfig(
        pg_table="zones",
        bq_final_table="raw_zones",
        merge_key="zone_id",
        watermark_field="updated_at",
        extraction_mode="full",
        lookback_days=0,
        source_order_fields=["updated_at"],
        cluster_fields=["city_id", "zone_type", "is_hotspot"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "zone_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "city_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "zone_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "zone_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "zone_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "is_hotspot", "type": "BOOL", "mode": "REQUIRED"},
            {"name": "is_active", "type": "BOOL", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            zone_id, city_id, zone_code, zone_name, zone_type,
            is_hotspot, is_active, created_at, updated_at
        """,
    ),

    # -------------------------------------------------------------------------
    # Mutable master/current-state tables: incremental MERGE by updated_at.
    # -------------------------------------------------------------------------
    TableConfig(
        pg_table="customers",
        bq_final_table="raw_customers",
        merge_key="customer_id",
        watermark_field="updated_at",
        extraction_mode="incremental",
        lookback_days=2,
        source_order_fields=["updated_at"],
        cluster_fields=["registered_city_id", "customer_status"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "customer_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "customer_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "registered_city_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "customer_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            customer_id, customer_name, registered_city_id, customer_status,
            created_at, updated_at
        """,
    ),
    TableConfig(
        pg_table="drivers",
        bq_final_table="raw_drivers",
        merge_key="driver_id",
        watermark_field="updated_at",
        extraction_mode="incremental",
        lookback_days=2,
        source_order_fields=["updated_at"],
        cluster_fields=["city_id", "service_type", "driver_status"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "driver_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "driver_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "city_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "service_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "driver_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "rating", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            driver_id, driver_name, city_id, service_type, driver_status,
            rating, created_at, updated_at
        """,
    ),
    TableConfig(
        pg_table="vehicles",
        bq_final_table="raw_vehicles",
        merge_key="vehicle_id",
        watermark_field="updated_at",
        extraction_mode="incremental",
        lookback_days=2,
        source_order_fields=["updated_at"],
        cluster_fields=["driver_id", "vehicle_type", "vehicle_status"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "vehicle_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "driver_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "vehicle_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "vehicle_year", "type": "INT64", "mode": "REQUIRED"},
            {"name": "vehicle_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            vehicle_id, driver_id, vehicle_type, vehicle_year, vehicle_status,
            created_at, updated_at
        """,
    ),

    # -------------------------------------------------------------------------
    # Lifecycle transactions: longer lookback because old business events can
    # receive a later status/payment update.
    # -------------------------------------------------------------------------
    TableConfig(
        pg_table="rides",
        bq_final_table="raw_rides",
        merge_key="ride_id",
        watermark_field="updated_at",
        extraction_mode="incremental",
        lookback_days=7,
        source_order_fields=["updated_at", "status_version"],
        version_field="status_version",
        bq_partition_field="requested_at",
        bq_partition_type="DAY",
        cluster_fields=["city_id", "service_type", "ride_status"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "ride_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "driver_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "city_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "service_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "pickup_zone_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "dropoff_zone_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "requested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "accepted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "driver_arrived_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "started_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "completed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "cancelled_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "cancelled_by", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cancellation_reason", "type": "STRING", "mode": "NULLABLE"},
            {"name": "estimated_distance_km", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "actual_distance_km", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "estimated_duration_min", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "actual_duration_min", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "base_fare", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "distance_fare", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "time_fare", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "surge_multiplier", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "gross_fare", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "discount_amount", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "final_fare", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "status_version", "type": "INT64", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            ride_id, customer_id, driver_id, city_id, service_type,
            pickup_zone_id, dropoff_zone_id, ride_status,
            requested_at, accepted_at, driver_arrived_at, started_at,
            completed_at, cancelled_at, cancelled_by, cancellation_reason,
            estimated_distance_km, actual_distance_km,
            estimated_duration_min, actual_duration_min,
            base_fare, distance_fare, time_fare, surge_multiplier,
            gross_fare, discount_amount, final_fare, status_version,
            created_at, updated_at
        """,
    ),
    TableConfig(
        pg_table="payments",
        bq_final_table="raw_payments",
        merge_key="payment_id",
        watermark_field="updated_at",
        extraction_mode="incremental",
        lookback_days=7,
        source_order_fields=["updated_at"],
        bq_partition_field="created_at",
        bq_partition_type="DAY",
        cluster_fields=["ride_id", "payment_method", "payment_status"],
        source_system="ride_hailing",
        schema_fields=[
            {"name": "payment_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_method", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_amount", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "platform_fee", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "driver_earning", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "failure_reason", "type": "STRING", "mode": "NULLABLE"},
            {"name": "paid_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
        table_columns="""
            payment_id, ride_id, payment_method, payment_status,
            payment_amount, platform_fee, driver_earning, failure_reason,
            paid_at, created_at, updated_at
        """,
    ),
]


def parse_boundary(value: str | None, default: pendulum.DateTime) -> pendulum.DateTime:
    """Interpret a naive manual boundary as Asia/Jakarta, then return UTC."""
    if not value:
        return default.in_timezone("UTC")

    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            "Use ISO format, for example 2026-08-01 00:00:00 or "
            "2026-08-01T00:00:00+07:00"
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(BUSINESS_TZ))

    return pendulum.instance(parsed).in_timezone("UTC")


def make_batch_id(run_id: str, start_utc: pendulum.DateTime, end_utc: pendulum.DateTime) -> str:
    compact_run = run_id.replace(":", "_").replace("+", "_")
    return (
        f"{compact_run}__{start_utc.format('YYYYMMDDTHHmmss')}"
        f"__{end_utc.format('YYYYMMDDTHHmmss')}"
    )[:250]


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    description=(
        "Time-based PostgreSQL to BigQuery current-state ingestion through Trino "
        f"({len(TABLE_CONFIGS)} tables)"
    ),
    default_args=default_args,
    schedule=CronDataIntervalTimetable("0 1 * * *", timezone=BUSINESS_TZ),
    start_date=pendulum.datetime(2026, 1, 1, tz=BUSINESS_TZ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["postgres", "bigquery", "trino", "timebased", "raw"],
    doc_md=__doc__,
    params={
        "window_start": Param(
            default=None,
            type=["null", "string"],
            description=(
                f"Inclusive base window start. Naive values use {BUSINESS_TZ}; "
                "ISO offsets are accepted. Per-table lookback is applied afterward."
            ),
        ),
        "window_end": Param(
            default=None,
            type=["null", "string"],
            description=(
                f"Exclusive window end. Naive values use {BUSINESS_TZ}; "
                "ISO offsets are accepted."
            ),
        ),
    },
) as dag:

    @task(task_id="build_window")
    def build_window() -> dict[str, str]:
        context = get_current_context()
        params = context["params"]
        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]

        start_utc = parse_boundary(params.get("window_start"), interval_start)
        end_utc = parse_boundary(params.get("window_end"), interval_end)
        if start_utc >= end_utc:
            raise ValueError("window_start must be earlier than window_end")

        run_id = context["dag_run"].run_id
        result = {
            "window_start_utc": start_utc.format("YYYY-MM-DD HH:mm:ss"),
            "window_end_utc": end_utc.format("YYYY-MM-DD HH:mm:ss"),
            "window_start_jakarta": start_utc.in_timezone(
                BUSINESS_TZ
            ).format("YYYY-MM-DD HH:mm:ss"),

            "window_end_jakarta": end_utc.in_timezone(
                BUSINESS_TZ
            ).format("YYYY-MM-DD HH:mm:ss"),
            "batch_id": make_batch_id(run_id, start_utc, end_utc),
        }
        print("Resolved ingestion window:", result)
        return result

    window = build_window()
    table_groups = [make_table_task_group(config, **SHARED) for config in TABLE_CONFIGS]

    # All tables may run in parallel after the window is resolved. The DAG-level
    # max_active_tasks limits pressure on the local PostgreSQL/Trino stack.
    window >> table_groups
