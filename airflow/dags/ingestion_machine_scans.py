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
    generate_date_list, get_json, load_ndjson_to_bigquery, make_bigquery_sql_task,
    new_temp_ndjson, pause, resolve_extract_window, upload_ndjson,
)

LOG = logging.getLogger(__name__)

DAG_ID = "ingestion_hallolaundry_machine_scans"
LIST_URL = "https://api-docs.hallolaundry.com/api/v2/iot/machines/scanning-histories"
PER_PAGE = 100


@dag(
    dag_id=DAG_ID,
    schedule="0 4 * * 0",  # Sunday 04:00 WIB
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={"owner": "data-engineering", "retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["haghi-laundry", "ingestion", "iot", "scans"],
)
def ingestion_hallolaundry_machine_scans():

    @task(execution_timeout=timedelta(hours=4))
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
            "Starting machine scan extraction mode=%s start_date=%s end_date=%s "
            "run_id=%s ingestion_date=%s",
            mode, start_date, end_date, run_id, ingestion_date,
        )

        try:
            authenticate(session)
            LOG.info("HalloLaundry authentication successful.")
            with new_temp_ndjson("hallolaundry_machine_scan_") as tmp:
                temp_path = tmp.name
                for target_date in generate_date_list(start_date, end_date):
                    LOG.info("Starting machine scan date=%s", target_date)
                    for page in range(1, MAX_PAGES_PER_RANGE + 1):
                        LOG.info(
                            "Fetching machine scans date=%s page=%s per_page=%s",
                            target_date, page, PER_PAGE,
                        )
                        payload = get_json(session, LIST_URL, params={
                            "start_date": target_date.isoformat(),
                            "end_date": target_date.isoformat(),
                            "page": page, "per_page": PER_PAGE,
                        })
                        record = payload.get("record") or {}
                        if not isinstance(record, dict):
                            raise AirflowException("Scanning API 'record' must be an object.")
                        histories = record.get("histories") or []
                        if not isinstance(histories, list):
                            raise AirflowException("Scanning API record.histories must be a list.")
                        if not histories:
                            LOG.info(
                                "Machine scan date=%s page=%s returned 0 rows. total_rows=%s",
                                target_date, page, row_count,
                            )
                            break
                        for row in histories:
                            tmp.write(json.dumps(add_ingestion_metadata(
                                row, run_id=run_id, ingestion_date=ingestion_date,
                                ingested_at=ingested_at, mode=mode,
                                start_date=start_date, end_date=end_date,
                            ), ensure_ascii=False) + "\n")
                            row_count += 1
                        LOG.info(
                            "Completed machine scan page date=%s page=%s page_rows=%s total_rows=%s",
                            target_date, page, len(histories), row_count,
                        )
                        if len(histories) < PER_PAGE:
                            LOG.info(
                                "Completed machine scan date=%s at page=%s (last page rows=%s).",
                                target_date, page, len(histories),
                            )
                            break
                        pause(PAGE_DELAY_SECONDS)
                    else:
                        raise AirflowException(f"Exceeded MAX pages for scan date {target_date}.")
            LOG.info(
                "Machine scan extraction completed total_rows=%s. Starting GCS upload.",
                row_count,
            )
            meta = upload_ndjson(
                temp_path=temp_path, entity="history_scanning_iot",
                raw_table="raw_history_scanning_iot", row_count=row_count,
                run_id=run_id, ingestion_date=ingestion_date, mode=mode,
                start_date=start_date, end_date=end_date,
            )
            LOG.info(
                "Machine scan extraction and GCS upload completed total_rows=%s",
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
    stg = make_bigquery_sql_task(
        "stg_machine_scan_events", "staging/07_stg_machine_scan_events.sql"
    )
    loaded >> stg


ingestion_hallolaundry_machine_scans()