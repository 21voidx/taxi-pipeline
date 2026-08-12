from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

SA_KEY_HOST = os.getenv(
    "SA_KEY_HOST",
    "/home/void/credentials/service-account.json",
)
SA_KEY_CONTAINER = "/opt/gcp/service-account.json"

DBT_PROJECT_HOST = os.getenv(
    "DBT_PROJECT_HOST",
    "/opt/data-platform-production/dbt/project",
)
DBT_IMAGE = os.getenv("DBT_IMAGE_NAME", "dbt-project:local")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")
DBT_BASE = f"--project-dir /app --profiles-dir /app --target {DBT_TARGET}"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

DOCKER_COMMON = {
    "image": DBT_IMAGE,
    "auto_remove": "force",
    "force_pull": True,
    "user": os.getenv("AIRFLOW_UID", "1001"),
    "mount_tmp_dir": False,
    "environment": {
        "ENVIRONMENT": os.getenv("ENVIRONMENT", "production"),
        "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID", "taxi-pipeline-484508"),
        "BQ_RAW_DATASET": os.getenv("BQ_RAW_DATASET", "prod_raw_ride_hailing"),
        "BQ_ANALYTICS_DATASET": os.getenv(
            "BQ_ANALYTICS_DATASET",
            "prod_analytics_ride_hailing",
        ),
        "BQ_MART_DATASET": os.getenv("BQ_MART_DATASET", "prod_mart_ride_hailing"),
        "DBT_TARGET": DBT_TARGET,
        "GOOGLE_APPLICATION_CREDENTIALS": SA_KEY_CONTAINER,
    },
    "mounts": [
        Mount(
            source=SA_KEY_HOST,
            target=SA_KEY_CONTAINER,
            type="bind",
            read_only=True,
        ),
        Mount(
            source=DBT_PROJECT_HOST,
            target="/app",
            type="bind",
        ),
    ],
}


with DAG(
    dag_id="dbt_build_daily",
    description="Build dbt daily models in the environment selected by DBT_TARGET.",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "bigquery", "transformation"],
    default_args=default_args,
) as dag:
    dbt_debug = DockerOperator(
        task_id="dbt_debug",
        command=f"dbt debug {DBT_BASE}",
        **DOCKER_COMMON,
    )

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        command=f"dbt deps {DBT_BASE}",
        **DOCKER_COMMON,
    )

    dbt_daily = DockerOperator(
        task_id="dbt_daily",
        command=f"dbt build --select tag:daily {DBT_BASE}",
        **DOCKER_COMMON,
    )

    dbt_debug >> dbt_deps >> dbt_daily
