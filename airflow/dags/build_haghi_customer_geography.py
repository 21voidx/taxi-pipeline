from __future__ import annotations

from datetime import timedelta

import pendulum

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

try:  # Airflow 3 / providers-standard
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except ImportError:  # Airflow 2 compatibility
    from airflow.sensors.external_task import ExternalTaskSensor

from helpers.customer_geography import geocode_new_customer_addresses
from helpers.hallolaundry import SQL_DIR, TIMEZONE, make_bigquery_sql_task


@dag(
    dag_id="build_haghi_customer_geography",
    # Intentionally identical to the analytics DAG schedule.
    # This gives both DAG runs the same logical date, so ExternalTaskSensor
    # can match the corresponding analytics run without execution_delta.
    schedule="0 11 * * 0",
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["haghi-laundry", "datamart", "customer-geography", "geocoding"],
)
def build_haghi_customer_geography():
    # Cross-DAG dependency.
    # Wait only for the upstream task this mart actually needs instead of
    # coupling geography to every task in build_haghi_customer_analytics.
    wait_for_customer_analytics = ExternalTaskSensor(
        task_id="wait_for_customer_analytics",
        external_dag_id="build_haghi_customer_analytics",
        external_task_id="mart_customer_analytics",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        timeout=2 * 60 * 60,
        poke_interval=60,
        deferrable=True,
    )

    # Geography branch can prepare/geocode addresses while the analytics DAG
    # is running because it depends only on the already-built STG customer table.
    primary_address = make_bigquery_sql_task(
        "int_customer_primary_address",
        "mart_customer_geography/27_int_customer_primary_address.sql",
    )

    @task(task_id="geocode_customer_addresses")
    def geocode_customer_addresses_task() -> dict:
        return geocode_new_customer_addresses()

    geocode_customer_addresses = geocode_customer_addresses_task()

    customer_geography = make_bigquery_sql_task(
        "mart_customer_geography",
        "mart_customer_geography/28_mart_customer_geography.sql",
    )

    primary_address >> geocode_customer_addresses
    [wait_for_customer_analytics, geocode_customer_addresses] >> customer_geography


build_haghi_customer_geography()