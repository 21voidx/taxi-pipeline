from __future__ import annotations

import os
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
# ─── Paths & constants ────────────────────────────────────────────────────────

# dbt project & profiles — di-mount ke Airflow worker via volume
DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")

# Target profile: prod untuk scheduled run, override saat manual trigger
DBT_TARGET       = os.getenv("DBT_TARGET", "prod")

# Slack alert — opsional, kosongkan untuk disable
SLACK_WEBHOOK    = os.getenv("SLACK_WEBHOOK_URL", "")


default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email":             [os.getenv("ALERT_EMAIL", "data-engineering@bank.co.id")],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    # "on_failure_callback": slack_alert,
}

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dbt_test",
    description=(
        "dbt BashOperator pipeline: source freshness → staging → marts → snapshots. "
        "Dijalankan setelah dag_core_banking dan dag_transactions selesai."
    ),
    schedule="@once",
    start_date=pendulum.datetime(2026, 3, 11, tz="Asia/Jakarta"),
    catchup=False,                    # backfill historical runs
    max_active_runs=1,               # cegah concurrent mart rebuild
    tags=["dbt", "bigquery", "transformation", "daily"],
    default_args=default_args,
    doc_md=__doc__
) as dag:
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-project-taxi:1.0",
        command='dbt debug --target dev',
        auto_remove="force", # 'never', 'success', or 'force'
        environment={
            "GCP_PROJECT_ID": "taxi-pipeline-123",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/gcp/service-account.json",
        },
        mounts=[
            Mount(
                source="/mnt/e/DE_PORTO_V2/Taxi-Pipeline/credentials/service-account.json",
                target="/opt/gcp/service-account.json",
                type="bind",
            ),
        ],
    )