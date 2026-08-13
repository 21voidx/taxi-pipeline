from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

import requests
from google.api_core.exceptions import NotFound
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:  # Airflow 3
    from airflow.sdk import Variable
except ImportError:  # Airflow 2 compatibility
    from airflow.models import Variable

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

LOG = logging.getLogger(__name__)

LOGIN_URL = "https://api-docs.hallolaundry.com/api/v2/auth/user"
USERNAME_VARIABLE = "HALLOLAUNDRY_USERNAME"
PASSWORD_VARIABLE = "HALLOLAUNDRY_PASSWORD"

GCP_CONN_ID = "google_cloud_haghi"
GCP_PROJECT_ID = os.getenv("HAGHI_GCP_PROJECT_ID", "dbt-taxi-explore")
BQ_RAW_DATASET = os.getenv("HAGHI_BQ_RAW_DATASET", "raw_haghi_laundry")
BQ_STG_DATASET = os.getenv("HAGHI_BQ_STG_DATASET", "stg_haghi_laundry")
BQ_MART_DATASET = os.getenv("HAGHI_BQ_MART_DATASET", "mart_haghi_laundry")
BQ_LOCATION = os.getenv("HAGHI_BQ_LOCATION", "US")
GCS_BUCKET_ENV = "HALLOLAUNDRY_GCS_BUCKET"
DAGS_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = DAGS_DIR / "sql"

TIMEZONE = "Asia/Jakarta"
LOOKBACK_MONTHS = 2
MAX_DAYS_PER_API_RANGE = 60
MAX_PAGES_PER_RANGE = 10_000
REQUEST_TIMEOUT = (10, 60)
PAGE_DELAY_SECONDS = float(os.getenv("HAGHI_API_PAGE_DELAY_SECONDS", "1.0"))
DETAIL_DELAY_SECONDS = float(os.getenv("HAGHI_API_DETAIL_DELAY_SECONDS", "1.0"))
RETRYABLE_HTTP_STATUS = (429, 500, 502, 503, 504)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}


def required_variable(name: str) -> str:
    value = Variable.get(name, default=None)
    if not value:
        raise AirflowException(f"Required environment variable {name!r} is not set.")
    return value


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise AirflowException(
            f"{field_name} must use YYYY-MM-DD, got {value!r}."
        ) from exc


def safe_identifier(value: str, max_length: int = 180) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")
    return (cleaned or "run")[:max_length]


def generate_date_ranges(start_date: date, end_date: date) -> Iterator[tuple[date, date]]:
    current_start = start_date
    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=MAX_DAYS_PER_API_RANGE - 1),
            end_date,
        )
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)


def generate_date_list(start_date: date, end_date: date) -> Iterator[date]:
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def resolve_extract_window(context: dict) -> tuple[date, date, str]:
    conf = context["dag_run"].conf or {}
    manual_start = conf.get("start_date")
    manual_end = conf.get("end_date")

    if bool(manual_start) != bool(manual_end):
        raise AirflowException(
            "Manual bootstrap/backfill requires both start_date and end_date."
        )

    if manual_start and manual_end:
        start_date = parse_iso_date(manual_start, "start_date")
        end_date = parse_iso_date(manual_end, "end_date")
        mode = "manual"
    else:
        interval_end = context["data_interval_end"].in_timezone(TIMEZONE)
        end_date = interval_end.date()
        start_date = interval_end.subtract(months=LOOKBACK_MONTHS).date()
        mode = "incremental"

    if start_date > end_date:
        raise AirflowException(
            f"start_date ({start_date}) cannot be after end_date ({end_date})."
        )
    return start_date, end_date, mode


def build_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=2,
        status_forcelist=RETRYABLE_HTTP_STATUS,
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def authenticate(session: requests.Session) -> None:
    username = required_variable("HALLOLAUNDRY_USERNAME")
    password = required_variable("HALLOLAUNDRY_PASSWORD")
    if not username or not password:
        raise AirflowException(
            f"Airflow Variables {USERNAME_VARIABLE!r} and {PASSWORD_VARIABLE!r} are required."
        )

    response = session.post(
        LOGIN_URL,
        json={"username": username, "password": password},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise AirflowException("HalloLaundry login returned a non-JSON response.") from exc

    token = payload.get("record")
    if payload.get("statusCode") != 200 or not token:
        raise AirflowException(
            f"HalloLaundry authentication failed: {payload.get('message') or 'unknown error'}"
        )
    session.headers["Authorization"] = f"Bearer {token}"


def get_json(
    session: requests.Session,
    url: str,
    *,
    params: dict | None = None,
    allow_reauth: bool = True,
) -> dict:
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if response.status_code in (401, 403) and allow_reauth:
        LOG.warning("API session expired; authenticating again and retrying request once.")
        authenticate(session)
        return get_json(session, url, params=params, allow_reauth=False)

    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise AirflowException(f"API returned non-JSON response for {url}.") from exc
    if not isinstance(payload, dict):
        raise AirflowException(f"API response for {url} must be a JSON object.")
    return payload


def add_ingestion_metadata(
    row: dict,
    *,
    run_id: str,
    ingestion_date: str,
    ingested_at: str,
    mode: str,
    start_date: date | None,
    end_date: date | None,
) -> dict:
    result = dict(row)
    result.update(
        {
            "_ingested_at": ingested_at,
            "_ingestion_date": ingestion_date,
            "_airflow_run_id": run_id,
            "_extract_mode": mode,
            "_extract_start_date": start_date.isoformat() if start_date else None,
            "_extract_end_date": end_date.isoformat() if end_date else None,
            "_source": "hallolaundry_api",
        }
    )
    return result


def new_temp_ndjson(prefix: str) -> NamedTemporaryFile:
    return NamedTemporaryFile(
        mode="w",
        suffix=".ndjson",
        prefix=prefix,
        encoding="utf-8",
        delete=False,
    )


def upload_ndjson(
    *,
    temp_path: str,
    entity: str,
    raw_table: str,
    row_count: int,
    run_id: str,
    ingestion_date: str,
    mode: str,
    start_date: date | None,
    end_date: date | None,
) -> dict:
    bucket = required_variable(GCS_BUCKET_ENV)
    safe_run = safe_identifier(run_id)
    object_name = (
        f"raw/hallolaundry/{entity}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={safe_run}/{entity}.ndjson"
    )

    GCSHook(gcp_conn_id=GCP_CONN_ID).upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=temp_path,
        mime_type="application/x-ndjson",
        num_max_attempts=3,
    )

    return {
        "entity": entity,
        "raw_table": raw_table,
        "gcs_bucket": bucket,
        "gcs_object": object_name,
        "gcs_uri": f"gs://{bucket}/{object_name}",
        "row_count": row_count,
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "extract_mode": mode,
        "extract_start_date": start_date.isoformat() if start_date else None,
        "extract_end_date": end_date.isoformat() if end_date else None,
    }


def cleanup_temp(*paths: str | None) -> None:
    for path in paths:
        if path:
            Path(path).unlink(missing_ok=True)


def load_ndjson_to_bigquery(meta: dict) -> dict:
    if int(meta["row_count"]) == 0:
        LOG.warning("%s extraction returned 0 rows; RAW load is a no-op.", meta["entity"])
        return {**meta, "bq_loaded": False, "bq_job_id": None}

    raw_table = meta["raw_table"]
    safe_run = safe_identifier(meta["run_id"], 180)
    job_id = safe_identifier(f"haghi_{raw_table}_{safe_run}", 220)
    destination = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{raw_table}"

    hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        use_legacy_sql=False,
    )

    try:
        existing_job = hook.get_job(
            job_id=job_id,
            project_id=GCP_PROJECT_ID,
            location=BQ_LOCATION,
        )
    except NotFound:
        existing_job = None

    if existing_job is not None:
        existing_job.result()
        if existing_job.error_result:
            raise AirflowException(
                f"Existing BigQuery load job failed: {existing_job.error_result}"
            )
        LOG.info("RAW load already completed for job_id=%s; skipping duplicate append.", job_id)
        return {
            **meta,
            "bq_loaded": True,
            "bq_job_id": job_id,
            "bq_destination": destination,
            "idempotent_reattach": True,
        }

    configuration = {
        "load": {
            "sourceUris": [meta["gcs_uri"]],
            "destinationTable": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BQ_RAW_DATASET,
                "tableId": raw_table,
            },
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "autodetect": True,
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND",
            "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
            "timePartitioning": {"type": "DAY"},
            "ignoreUnknownValues": False,
            "maxBadRecords": 0,
        }
    }

    job = hook.insert_job(
        configuration=configuration,
        job_id=job_id,
        project_id=GCP_PROJECT_ID,
        location=BQ_LOCATION,
        nowait=False,
    )
    if job.error_result:
        raise AirflowException(f"BigQuery RAW load failed: {job.error_result}")

    return {
        **meta,
        "bq_loaded": True,
        "bq_job_id": job_id,
        "bq_destination": destination,
        "idempotent_reattach": False,
    }


def make_bigquery_sql_task(task_id: str, sql_file: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": "{% include '" + sql_file + "' %}",
                "useLegacySql": False,
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "raw_dataset": BQ_RAW_DATASET,
            "stg_dataset": BQ_STG_DATASET,
            "mart_dataset": BQ_MART_DATASET,
        },
    )


def pause(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
