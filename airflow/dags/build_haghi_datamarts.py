from __future__ import annotations

from datetime import timedelta

import pendulum

try:
    from airflow.sdk import dag
except ImportError:
    from airflow.decorators import dag

from helpers.hallolaundry import SQL_DIR, TIMEZONE, make_bigquery_sql_task


@dag(
    dag_id="build_haghi_datamarts",
    schedule="0 10 * * 0",  # Sunday 10:00 WIB, after weekly ingestion
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["haghi-laundry", "datamart"],
)
def build_haghi_datamarts():
    unit_reference = make_bigquery_sql_task(
        "stg_regular_service_unit_reference",
        "staging/13_stg_regular_service_unit_reference.sql",
    )

    business = make_bigquery_sql_task(
        "mart_daily_business_performance",
        "mart/20_mart_daily_business_performance.sql",
    )
    service = make_bigquery_sql_task(
        "mart_daily_service_performance",
        "mart/21_mart_daily_service_performance.sql",
    )
    unit = make_bigquery_sql_task(
        "mart_daily_unit_performance",
        "mart/24_mart_daily_unit_performance.sql",
    )

    unit_reference >> [business, service, unit]


build_haghi_datamarts()
