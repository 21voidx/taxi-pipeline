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


# ---------------------------------------------------------------------------
# Explicit BigQuery RAW schema contract
#
# Source of truth for required fields:
# haghi_laundry_datamart(2).sql / current staging SQL.
#
# Design:
# - scalar leaves are STRING in RAW;
# - nested objects remain RECORD;
# - arrays used by STG with UNNEST remain REPEATED RECORD;
# - source fields not listed here are ignored by BigQuery RAW loading but
#   remain present in the GCS NDJSON landing file.
# ---------------------------------------------------------------------------

def _string_field(name: str, mode: str = "NULLABLE") -> dict:
    return {"name": name, "type": "STRING", "mode": mode}


def _record_field(
    name: str,
    fields: list[dict],
    mode: str = "NULLABLE",
) -> dict:
    return {
        "name": name,
        "type": "RECORD",
        "mode": mode,
        "fields": fields,
    }


INGESTION_METADATA_SCHEMA = [
    _string_field("_ingested_at"),
    _string_field("_ingestion_date"),
    _string_field("_airflow_run_id"),
    _string_field("_extract_mode"),
    _string_field("_extract_start_date"),
    _string_field("_extract_end_date"),
    _string_field("_source"),
]


RAW_TRANSACTION_SCHEMA = [
    _string_field("id"),
    _string_field("note_number"),
    _string_field("ref_id"),
    _string_field("company_id"),
    _string_field("company_customer_id"),
    _record_field(
        "customer",
        [
            _string_field("uuid"),
            _string_field("name"),
        ],
    ),
    _string_field("company_outlet_id"),
    _record_field("outlet", [_string_field("name")]),
    _string_field("user_employee_id"),
    _record_field(
        "cashier",
        [
            _string_field("id"),
            _string_field("name"),
        ],
    ),
    _string_field("transaction_type"),
    _string_field("payment_status"),
    _string_field("transaction_status"),
    _string_field("transaction_status_text"),
    _string_field("position"),
    _string_field("is_late"),
    _string_field("is_delivery"),
    _string_field("is_express"),
    _string_field("company_outlet_express_service_id"),
    _string_field("transaction_progress"),
    _string_field("workshop_progress"),
    _string_field("estimation_finish_at"),
    _string_field("created_at"),
    _string_field("updated_at"),
    *INGESTION_METADATA_SCHEMA,
]


RAW_TRANSACTION_DETAIL_SCHEMA = [
    _string_field("id"),
    _string_field("note_number"),
    _string_field("ref_id"),
    _string_field("company_id"),
    _string_field("company_customer_id"),
    _record_field(
        "customer",
        [
            _string_field("uuid"),
            _string_field("name"),
            _string_field("is_membership_deposit"),
        ],
    ),
    _string_field("company_outlet_id"),
    _record_field("outlet", [_string_field("name")]),
    _string_field("user_employee_id"),
    _string_field("transaction_type"),
    _string_field("payment_status"),
    _string_field("transaction_status"),
    _string_field("transaction_status_text"),
    _string_field("position"),
    _string_field("is_late"),
    _string_field("is_delivery"),
    _string_field("company_outlet_express_service_id"),
    _string_field("transaction_progress"),
    _string_field("workshop_progress"),
    _string_field("created_at"),
    _string_field("updated_at"),
    _string_field("estimation_finish_at"),
    _string_field("request_cancelled_at"),
    _string_field("taking_at"),
    _string_field("cancelled_at"),
    _string_field("request_cancelled_reason"),

    # Header + regular.transaction_services[] used by:
    # 03_stg_regular_transactions.sql
    # 08_stg_regular_transaction_lines.sql
    _record_field(
        "regular",
        [
            _string_field("id"),
            _string_field("payment_type"),
            _string_field("amount"),
            _string_field("net_amount"),
            _string_field("net_amount_final"),
            _string_field("gross_income"),
            _string_field("discount"),
            _string_field("discount_regular_services"),
            _string_field("ppn"),
            _string_field("additional_price_amount"),
            _string_field("coin"),
            _string_field("amount_paid"),
            _string_field("amount_remaining"),
            _string_field("normal_price"),
            _string_field("express_price"),
            _record_field(
                "transaction_services",
                [
                    _string_field("id"),
                    _string_field("quantity"),
                    _string_field("amount"),
                    _string_field("discount"),
                    _string_field("net_amount"),
                    _string_field("sub_total"),
                    _string_field("created_at"),
                    _string_field("updated_at"),
                    _record_field(
                        "service",
                        [
                            _string_field("id"),
                            _string_field("name"),
                            _string_field("is_can_scanning"),
                            _string_field("multiply_scanning"),
                            _record_field(
                                "category",
                                [
                                    _string_field("id"),
                                    _string_field("name"),
                                ],
                            ),
                            _record_field(
                                "master_unit",
                                [
                                    _string_field("id"),
                                    _string_field("name"),
                                ],
                            ),
                        ],
                    ),
                ],
                mode="REPEATED",
            ),
        ],
    ),

    # Header + package.transaction_services[] used by:
    # 04_stg_package_transactions.sql
    # 09_stg_package_transaction_lines.sql
    _record_field(
        "package",
        [
            _string_field("id"),
            _string_field("amount"),
            _string_field("net_amount"),
            _string_field("ppn"),
            _string_field("coin"),
            _record_field(
                "transaction_services",
                [
                    _string_field("id"),
                    _string_field("company_customer_deposit_package_id"),
                    _string_field("company_outlet_regular_service_deposit_id"),
                    _string_field("quantity"),
                    _string_field("amount"),
                    _string_field("created_at"),
                    _string_field("updated_at"),
                    _record_field(
                        "service",
                        [
                            _string_field("name"),
                            _string_field("quantity"),
                            _string_field("discount"),
                            _string_field("price"),
                            _string_field("base_price"),
                            _string_field("expired_in_days"),
                            _record_field(
                                "regular_service",
                                [
                                    _string_field("id"),
                                    _string_field("name"),
                                    _string_field("price"),
                                    _string_field("is_can_scanning"),
                                    _string_field("multiply_scanning"),
                                    _record_field(
                                        "category",
                                        [
                                            _string_field("id"),
                                            _string_field("name"),
                                        ],
                                    ),
                                    _record_field(
                                        "master_unit",
                                        [
                                            _string_field("id"),
                                            _string_field("name"),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
                mode="REPEATED",
            ),
        ],
    ),

    # relation_services[] used by:
    # 12_stg_transaction_service_operations.sql
    _record_field(
        "relation_services",
        [
            _string_field("id"),
            _string_field("transaction_service_id"),
            _string_field("transaction_type"),
            _string_field("company_outlet_regular_service_id"),
            _string_field("company_outlet_regular_service_deposit_id"),
            _string_field("master_unit_regular_service_id"),
            _string_field("estimation_finish_at"),
            _string_field("created_at"),
            _string_field("updated_at"),
            _record_field(
                "service",
                [
                    _string_field("name"),
                    _string_field("is_can_scanning"),
                    _string_field("multiply_scanning"),
                    _record_field(
                        "master_unit",
                        [_string_field("name")],
                    ),
                    _record_field(
                        "regular_service",
                        [
                            _string_field("is_can_scanning"),
                            _string_field("multiply_scanning"),
                            _record_field(
                                "master_unit",
                                [_string_field("name")],
                            ),
                        ],
                    ),
                ],
            ),
            _record_field(
                "rack",
                [
                    _string_field("id"),
                    _string_field("combined_name"),
                    _string_field("number_rack"),
                ],
            ),
        ],
        mode="REPEATED",
    ),

    # detail_payment is intentionally excluded:
    # it is not referenced by current STG/MART SQL and contains polymorphic
    # fields such as double_payments (STRING in old schema, ARRAY in new data).
    *INGESTION_METADATA_SCHEMA,
]


def stringify_scalar_values(value):
    """Keep dict/list shape but serialize every scalar leaf as STRING."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {
            key: stringify_scalar_values(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [stringify_scalar_values(item) for item in value]
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


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

        LOG.info(
            "Starting transaction extraction mode=%s start_date=%s end_date=%s "
            "run_id=%s ingestion_date=%s",
            mode,
            start_date,
            end_date,
            run_id,
            ingestion_date,
        )

        try:
            authenticate(session)
            LOG.info("HalloLaundry authentication successful.")

            with new_temp_ndjson("hallolaundry_transaction_") as list_file, new_temp_ndjson(
                "hallolaundry_transaction_detail_"
            ) as detail_file:
                list_path = list_file.name
                detail_path = detail_file.name

                for range_start, range_end in generate_date_ranges(start_date, end_date):
                    LOG.info(
                        "Starting API range %s..%s",
                        range_start,
                        range_end,
                    )

                    for page in range(1, MAX_PAGES_PER_RANGE + 1):
                        LOG.info(
                            "Fetching transaction page range=%s..%s page=%s per_page=%s",
                            range_start,
                            range_end,
                            page,
                            PER_PAGE,
                        )

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
                                        stringify_scalar_values(trx),
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
                                            stringify_scalar_values(detail),
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

                                # Detail endpoint is intentionally rate-limited and can take
                                # several minutes per list page. Emit periodic progress so the
                                # Airflow task log does not look idle while detail scraping runs.
                                if detail_rows % 25 == 0:
                                    LOG.info(
                                        "Transaction detail progress "
                                        "range=%s..%s page=%s detail_rows=%s "
                                        "unique_transaction_ids=%s",
                                        range_start,
                                        range_end,
                                        page,
                                        detail_rows,
                                        len(seen_ids),
                                    )

                        LOG.info(
                            "Completed transaction page "
                            "range=%s..%s page=%s page_rows=%s "
                            "list_rows=%s detail_rows=%s",
                            range_start,
                            range_end,
                            page,
                            len(records),
                            list_rows,
                            detail_rows,
                        )
                        if len(records) < PER_PAGE:
                            LOG.info(
                                "Completed API range %s..%s at page=%s "
                                "(last page rows=%s).",
                                range_start,
                                range_end,
                                page,
                                len(records),
                            )
                            break

                        pause(PAGE_DELAY_SECONDS)
                    else:
                        raise AirflowException(
                            f"Exceeded MAX_PAGES_PER_RANGE for {range_start}..{range_end}."
                        )

            LOG.info(
                "Transaction extraction completed. "
                "list_rows=%s detail_rows=%s unique_transaction_ids=%s. "
                "Starting GCS upload.",
                list_rows,
                detail_rows,
                len(seen_ids),
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
            LOG.info(
                "Transaction list upload completed. Starting transaction detail upload."
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
            LOG.info(
                "Transaction extraction and GCS upload completed. "
                "list_rows=%s detail_rows=%s",
                list_rows,
                detail_rows,
            )
            return {"transaction": list_meta, "transaction_detail": detail_meta}
        finally:
            session.close()
            cleanup_temp(list_path, detail_path)

    @task(execution_timeout=timedelta(minutes=30))
    def load_transaction(meta: dict) -> dict:
        return load_ndjson_to_bigquery(
            meta["transaction"],
            schema=RAW_TRANSACTION_SCHEMA,
            ignore_unknown_values=True,
            job_version="v3_explicit_schema",
        )

    @task(execution_timeout=timedelta(minutes=30))
    def load_transaction_detail(meta: dict) -> dict:
        return load_ndjson_to_bigquery(
            meta["transaction_detail"],
            schema=RAW_TRANSACTION_DETAIL_SCHEMA,
            ignore_unknown_values=True,
            job_version="v3_explicit_schema",
        )

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