from __future__ import annotations

import json
import logging
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
    generate_date_ranges, get_json, load_ndjson_to_bigquery, make_bigquery_sql_task,
    new_temp_ndjson, pause, resolve_extract_window, upload_ndjson,
)

LOG = logging.getLogger(__name__)

DAG_ID = "ingestion_hallolaundry_deposit_purchases"
LIST_URL = "https://api-docs.hallolaundry.com/api/v2/transactions/deposit-purchases"
PER_PAGE = 100


@dag(
    dag_id=DAG_ID,
    schedule="30 5 * * 0",  # Sunday 05:30 WIB
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={"owner": "data-engineering", "retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["haghi-laundry", "ingestion", "deposit"],
)
def ingestion_hallolaundry_deposit_purchases():

    @task(execution_timeout=timedelta(hours=2))
    def extract_to_gcs() -> dict:
        context = get_current_context()
        run_id = context["run_id"]
        ingestion_date = context["data_interval_end"].in_timezone(TIMEZONE).date().isoformat()
        start_date, end_date, mode = resolve_extract_window(context)
        ingested_at = pendulum.now("UTC").to_iso8601_string()
        temp_path = None
        row_count = 0
        session = build_http_session()

        LOG.info(
            "Starting deposit purchase extraction mode=%s start_date=%s end_date=%s "
            "run_id=%s ingestion_date=%s",
            mode, start_date, end_date, run_id, ingestion_date,
        )

        try:
            authenticate(session)
            LOG.info("HalloLaundry authentication successful.")
            with new_temp_ndjson("hallolaundry_deposit_purchase_") as tmp:
                temp_path = tmp.name
                for range_start, range_end in generate_date_ranges(start_date, end_date):
                    LOG.info("Starting deposit API range %s..%s", range_start, range_end)
                    for page in range(1, MAX_PAGES_PER_RANGE + 1):
                        LOG.info(
                            "Fetching deposit purchases range=%s..%s page=%s per_page=%s",
                            range_start, range_end, page, PER_PAGE,
                        )
                        payload = get_json(session, LIST_URL, params={
                            "start_date": range_start.isoformat(),
                            "end_date": range_end.isoformat(),
                            "page": page, "per_page": PER_PAGE,
                        })
                        records = payload.get("record")
                        if not isinstance(records, list):
                            raise AirflowException("Deposit API 'record' must be a list.")
                        if not records:
                            LOG.info(
                                "Deposit API range=%s..%s page=%s returned 0 rows. total_rows=%s",
                                range_start, range_end, page, row_count,
                            )
                            break
                        for row in records:
                            tmp.write(json.dumps(add_ingestion_metadata(
                                row, run_id=run_id, ingestion_date=ingestion_date,
                                ingested_at=ingested_at, mode=mode,
                                start_date=start_date, end_date=end_date,
                            ), ensure_ascii=False) + "\n")
                            row_count += 1
                        LOG.info(
                            "Completed deposit page range=%s..%s page=%s page_rows=%s total_rows=%s",
                            range_start, range_end, page, len(records), row_count,
                        )
                        if len(records) < PER_PAGE:
                            LOG.info(
                                "Completed deposit API range %s..%s at page=%s (last page rows=%s).",
                                range_start, range_end, page, len(records),
                            )
                            break
                        pause(PAGE_DELAY_SECONDS)
                    else:
                        raise AirflowException("Exceeded MAX_PAGES_PER_RANGE for deposit API.")
            LOG.info(
                "Deposit purchase extraction completed total_rows=%s. Starting GCS upload.",
                row_count,
            )
            meta = upload_ndjson(
                temp_path=temp_path, entity="deposit_purchase", raw_table="raw_deposit_purchase",
                row_count=row_count, run_id=run_id, ingestion_date=ingestion_date,
                mode=mode, start_date=start_date, end_date=end_date,
            )
            LOG.info(
                "Deposit purchase extraction and GCS upload completed total_rows=%s",
                row_count,
            )
            return meta
        finally:
            session.close()
            cleanup_temp(temp_path)

    @task(execution_timeout=timedelta(minutes=30))
    def load_raw(meta: dict) -> dict:
        return load_ndjson_to_bigquery(meta)

    loaded = load_raw(extract_to_gcs())
    stg_header = make_bigquery_sql_task(
        "stg_deposit_purchases", "staging/06_stg_deposit_purchases.sql"
    )
    stg_lines = make_bigquery_sql_task(
        "stg_deposit_purchase_lines", "staging/11_stg_deposit_purchase_lines.sql"
    )
    loaded >> [stg_header, stg_lines]


ingestion_hallolaundry_deposit_purchases()