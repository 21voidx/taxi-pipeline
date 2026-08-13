from __future__ import annotations

import json
from datetime import timedelta

import pendulum

try:
    from airflow.sdk import dag, task, get_current_context
except ImportError:
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

from airflow.exceptions import AirflowException

from helpers.hallolaundry import (
    MAX_PAGES_PER_RANGE, PAGE_DELAY_SECONDS, SQL_DIR, TIMEZONE,
    add_ingestion_metadata, authenticate, build_http_session, cleanup_temp,
    get_json, load_ndjson_to_bigquery, make_bigquery_sql_task, new_temp_ndjson,
    pause, upload_ndjson,
)

DAG_ID = "ingestion_hallolaundry_customers"
LIST_URL = "https://api-docs.hallolaundry.com/api/v2/customers/list"
PER_PAGE = 500


@dag(
    dag_id=DAG_ID,
    schedule="0 6 * * 0",  # Sunday 06:00 WIB
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={"owner": "data-engineering", "retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["haghi-laundry", "ingestion", "customers"],
)
def ingestion_hallolaundry_customers():

    @task(execution_timeout=timedelta(hours=1))
    def extract_to_gcs() -> dict:
        context = get_current_context()
        run_id = context["run_id"]
        ingestion_date = context["data_interval_end"].in_timezone(TIMEZONE).date().isoformat()
        ingested_at = pendulum.now("UTC").to_iso8601_string()
        temp_path = None
        row_count = 0
        session = build_http_session()
        try:
            authenticate(session)
            with new_temp_ndjson("hallolaundry_customer_") as tmp:
                temp_path = tmp.name
                for page in range(1, MAX_PAGES_PER_RANGE + 1):
                    payload = get_json(session, LIST_URL, params={
                        "per_page": PER_PAGE, "page": page,
                        "sort": "customer_name", "order": "DESC",
                    })
                    data = payload.get("data") or {}
                    records = data.get("record") if isinstance(data, dict) else None
                    if records is None:
                        raise AirflowException("Customer API response has no data.record field.")
                    if not isinstance(records, list):
                        raise AirflowException("Customer API data.record must be a list.")
                    if not records:
                        break
                    for row in records:
                        tmp.write(json.dumps(add_ingestion_metadata(
                            row, run_id=run_id, ingestion_date=ingestion_date,
                            ingested_at=ingested_at, mode="snapshot",
                            start_date=None, end_date=None,
                        ), ensure_ascii=False) + "\n")
                        row_count += 1
                    if len(records) < PER_PAGE:
                        break
                    pause(PAGE_DELAY_SECONDS)
                else:
                    raise AirflowException("Exceeded customer pagination safety limit.")
            return upload_ndjson(
                temp_path=temp_path, entity="customer", raw_table="raw_customer",
                row_count=row_count, run_id=run_id, ingestion_date=ingestion_date,
                mode="snapshot", start_date=None, end_date=None,
            )
        finally:
            session.close()
            cleanup_temp(temp_path)

    @task(execution_timeout=timedelta(minutes=30))
    def load_raw(meta: dict) -> dict:
        return load_ndjson_to_bigquery(meta)

    loaded = load_raw(extract_to_gcs())
    stg = make_bigquery_sql_task("stg_customer", "staging/02_stg_customer.sql")
    loaded >> stg


ingestion_hallolaundry_customers()
