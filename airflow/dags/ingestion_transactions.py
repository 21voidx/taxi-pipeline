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
    DETAIL_DELAY_SECONDS,
    GCP_PROJECT_ID,
    MAX_PAGES_PER_RANGE,
    PAGE_DELAY_SECONDS,
    SQL_DIR,
    TIMEZONE,
    add_ingestion_metadata,
    authenticate,
    build_http_session,
    cleanup_temp,
    generate_date_ranges,
    get_json,
    load_ndjson_to_bigquery,
    make_bigquery_sql_task,
    new_temp_ndjson,
    pause,
    resolve_extract_window,
    upload_ndjson,
)

LOG = logging.getLogger(__name__)
DAG_ID = "ingestion_hallolaundry_transactions"
LIST_URL = "https://api-docs.hallolaundry.com/api/v2/transactions"
DETAIL_BASE_URL = "https://api-docs.hallolaundry.com/api/v2/transactions/"
PER_PAGE = 100


@dag(
    dag_id=DAG_ID,
    schedule="0 0 * * 0",  # Sunday 00:00 WIB
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    tags=["haghi-laundry", "ingestion", "transactions"],
)
def ingestion_hallolaundry_transactions():

    @task(execution_timeout=timedelta(hours=6))
    def extract_to_gcs() -> dict:
        context = get_current_context()
        run_id = context["run_id"]
        interval_end_local = context["data_interval_end"].in_timezone(TIMEZONE)
        ingestion_date = interval_end_local.date().isoformat()
        start_date, end_date, mode = resolve_extract_window(context)
        ingested_at = pendulum.now("UTC").to_iso8601_string()

        list_path = detail_path = None
        list_rows = detail_rows = 0
        seen_ids: set[str] = set()
        session = build_http_session()

        try:
            authenticate(session)
            with new_temp_ndjson("hallolaundry_transaction_") as list_file, new_temp_ndjson(
                "hallolaundry_transaction_detail_"
            ) as detail_file:
                list_path = list_file.name
                detail_path = detail_file.name

                for range_start, range_end in generate_date_ranges(start_date, end_date):
                    for page in range(1, MAX_PAGES_PER_RANGE + 1):
                        payload = get_json(
                            session,
                            LIST_URL,
                            params={
                                "start_date": range_start.isoformat(),
                                "end_date": range_end.isoformat(),
                                "order_by": "desc",
                                "search": "",
                                "page": page,
                                "per_page": PER_PAGE,
                            },
                        )
                        records = payload.get("record")
                        if not isinstance(records, list):
                            raise AirflowException("Transactions API 'record' must be a list.")
                        if not records:
                            break

                        for trx in records:
                            if not isinstance(trx, dict):
                                raise AirflowException("Transaction list contains a non-object row.")
                            trx_id = str(trx.get("id") or "").strip()
                            if not trx_id:
                                raise AirflowException("Transaction list row has no id.")

                            list_file.write(
                                json.dumps(
                                    add_ingestion_metadata(
                                        trx,
                                        run_id=run_id,
                                        ingestion_date=ingestion_date,
                                        ingested_at=ingested_at,
                                        mode=mode,
                                        start_date=start_date,
                                        end_date=end_date,
                                    ),
                                    ensure_ascii=False,
                                )
                                + "\n"
                            )
                            list_rows += 1

                            if trx_id not in seen_ids:
                                pause(DETAIL_DELAY_SECONDS)
                                detail_payload = get_json(session, f"{DETAIL_BASE_URL}{trx_id}")
                                detail = detail_payload.get("record")
                                if not isinstance(detail, dict) or not detail:
                                    raise AirflowException(
                                        f"Transaction detail is empty/invalid for id={trx_id}."
                                    )
                                detail_file.write(
                                    json.dumps(
                                        add_ingestion_metadata(
                                            detail,
                                            run_id=run_id,
                                            ingestion_date=ingestion_date,
                                            ingested_at=ingested_at,
                                            mode=mode,
                                            start_date=start_date,
                                            end_date=end_date,
                                        ),
                                        ensure_ascii=False,
                                    )
                                    + "\n"
                                )
                                detail_rows += 1
                                seen_ids.add(trx_id)

                        LOG.info(
                            "range=%s..%s page=%s list_rows=%s detail_rows=%s",
                            range_start,
                            range_end,
                            page,
                            list_rows,
                            detail_rows,
                        )
                        if len(records) < PER_PAGE:
                            break
                        pause(PAGE_DELAY_SECONDS)
                    else:
                        raise AirflowException(
                            f"Exceeded MAX_PAGES_PER_RANGE for {range_start}..{range_end}."
                        )

            list_meta = upload_ndjson(
                temp_path=list_path,
                entity="transaction",
                raw_table="raw_transaction",
                row_count=list_rows,
                run_id=run_id,
                ingestion_date=ingestion_date,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
            )
            detail_meta = upload_ndjson(
                temp_path=detail_path,
                entity="transaction_detail",
                raw_table="raw_transaction_detail",
                row_count=detail_rows,
                run_id=run_id,
                ingestion_date=ingestion_date,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
            )
            return {"transaction": list_meta, "transaction_detail": detail_meta}
        finally:
            session.close()
            cleanup_temp(list_path, detail_path)

    @task(execution_timeout=timedelta(minutes=30))
    def load_transaction(meta: dict) -> dict:
        return load_ndjson_to_bigquery(meta["transaction"])

    @task(execution_timeout=timedelta(minutes=30))
    def load_transaction_detail(meta: dict) -> dict:
        return load_ndjson_to_bigquery(meta["transaction_detail"])

    extracted = extract_to_gcs()
    raw_transaction = load_transaction(extracted)
    raw_detail = load_transaction_detail(extracted)

    stg_index = make_bigquery_sql_task(
        "stg_transaction_index", "staging/01_stg_transaction_index.sql"
    )
    stg_regular = make_bigquery_sql_task(
        "stg_regular_transactions", "staging/03_stg_regular_transactions.sql"
    )
    stg_package = make_bigquery_sql_task(
        "stg_package_transactions", "staging/04_stg_package_transactions.sql"
    )
    stg_regular_lines = make_bigquery_sql_task(
        "stg_regular_transaction_lines", "staging/08_stg_regular_transaction_lines.sql"
    )
    stg_package_lines = make_bigquery_sql_task(
        "stg_package_transaction_lines", "staging/09_stg_package_transaction_lines.sql"
    )
    stg_ops = make_bigquery_sql_task(
        "stg_transaction_service_operations",
        "staging/12_stg_transaction_service_operations.sql",
    )

    raw_transaction >> stg_index
    raw_detail >> [stg_regular_lines, stg_package_lines, stg_ops]
    [raw_detail, stg_index] >> stg_regular
    [raw_detail, stg_index] >> stg_package


ingestion_hallolaundry_transactions()
