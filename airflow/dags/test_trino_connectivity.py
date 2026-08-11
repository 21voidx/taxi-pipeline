"""
DAG smoke test untuk memvalidasi konektivitas:

Airflow -> Trino
Trino -> PostgreSQL catalog
Trino -> BigQuery catalog
Optional: membaca tabel BigQuery dan menjalankan cross-catalog query.

DAG ini manual-only dan tidak menulis/mengubah data.
"""

from __future__ import annotations

import os
import re
from datetime import timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator


DAG_ID = "test_trino_connectivity"
BUSINESS_TZ = "Asia/Jakarta"

TRINO_CONN_ID = os.getenv("AIRFLOW_TRINO_CONN_ID", "trino_default")
TRINO_PG_CATALOG = os.getenv("TRINO_CATALOG_POSTGRES", "postgresql")
TRINO_BQ_CATALOG = os.getenv("TRINO_CATALOG_BIGQUERY", "bigquery")

POSTGRES_SCHEMA = os.getenv("POSTGRES_SOURCE_SCHEMA", "public")
POSTGRES_TEST_TABLE = os.getenv("TRINO_SMOKE_POSTGRES_TABLE", "cities")

BIGQUERY_SCHEMA = os.getenv("BQ_RAW_DATASET", "raw_ride_hailing")
BIGQUERY_TEST_TABLE = os.getenv("TRINO_SMOKE_BIGQUERY_TABLE", "raw_cities")


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


ENABLE_BIGQUERY_TABLE_TEST = env_bool(
    "TRINO_SMOKE_ENABLE_BIGQUERY_TABLE_TEST",
    default=False,
)
ENABLE_CROSS_CATALOG_TEST = env_bool(
    "TRINO_SMOKE_ENABLE_CROSS_CATALOG_TEST",
    default=False,
)


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def identifier(value: str, label: str) -> str:
    """Validate identifiers interpolated into Trino SQL."""
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid {label}={value!r}. Use letters, numbers, and underscores only, "
            "and do not start with a number."
        )
    return value


PG_CATALOG = identifier(TRINO_PG_CATALOG, "TRINO_CATALOG_POSTGRES")
BQ_CATALOG = identifier(TRINO_BQ_CATALOG, "TRINO_CATALOG_BIGQUERY")
PG_SCHEMA = identifier(POSTGRES_SCHEMA, "POSTGRES_SOURCE_SCHEMA")
PG_TABLE = identifier(POSTGRES_TEST_TABLE, "TRINO_SMOKE_POSTGRES_TABLE")
BQ_SCHEMA = identifier(BIGQUERY_SCHEMA, "BQ_RAW_DATASET")
BQ_TABLE = identifier(BIGQUERY_TEST_TABLE, "TRINO_SMOKE_BIGQUERY_TABLE")


def sql_string(value: str) -> str:
    return value.replace("'", "''")


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    description="Smoke test Airflow -> Trino -> PostgreSQL and BigQuery",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=BUSINESS_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["test", "smoke-test", "trino", "postgres", "bigquery"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    test_trino_engine = SQLExecuteQueryOperator(
        task_id="test_trino_engine",
        conn_id=TRINO_CONN_ID,
        sql="""
            SELECT
                version() AS trino_version,
                current_user AS trino_user,
                current_timestamp AS tested_at
        """,
        autocommit=True,
        do_xcom_push=True,
    )

    with TaskGroup(group_id="postgres_connector") as postgres_connector:
        list_postgres_schemas = SQLExecuteQueryOperator(
            task_id="list_schemas",
            conn_id=TRINO_CONN_ID,
            sql=f"SHOW SCHEMAS FROM {PG_CATALOG}",
            autocommit=True,
            do_xcom_push=True,
        )

        inspect_postgres_schema = SQLExecuteQueryOperator(
            task_id="inspect_schema",
            conn_id=TRINO_CONN_ID,
            sql=f"""
                SELECT
                    table_schema,
                    table_name,
                    table_type
                FROM {PG_CATALOG}.information_schema.tables
                WHERE table_schema = '{sql_string(PG_SCHEMA)}'
                ORDER BY table_name
                LIMIT 50
            """,
            autocommit=True,
            do_xcom_push=True,
        )

        read_postgres_table = SQLExecuteQueryOperator(
            task_id="read_test_table",
            conn_id=TRINO_CONN_ID,
            sql=f"""
                SELECT
                    COUNT(*) AS row_count
                FROM {PG_CATALOG}.{PG_SCHEMA}.{PG_TABLE}
            """,
            autocommit=True,
            do_xcom_push=True,
        )

        list_postgres_schemas >> inspect_postgres_schema >> read_postgres_table

    with TaskGroup(group_id="bigquery_connector") as bigquery_connector:
        list_bigquery_schemas = SQLExecuteQueryOperator(
            task_id="list_schemas",
            conn_id=TRINO_CONN_ID,
            sql=f"SHOW SCHEMAS FROM {BQ_CATALOG}",
            autocommit=True,
            do_xcom_push=True,
        )

        inspect_bigquery_schema = SQLExecuteQueryOperator(
            task_id="inspect_schema",
            conn_id=TRINO_CONN_ID,
            sql=f"""
                SELECT
                    table_schema,
                    table_name,
                    table_type
                FROM {BQ_CATALOG}.information_schema.tables
                WHERE table_schema = '{sql_string(BQ_SCHEMA)}'
                ORDER BY table_name
                LIMIT 50
            """,
            autocommit=True,
            do_xcom_push=True,
        )

        list_bigquery_schemas >> inspect_bigquery_schema

        if ENABLE_BIGQUERY_TABLE_TEST:
            read_bigquery_table = SQLExecuteQueryOperator(
                task_id="read_test_table",
                conn_id=TRINO_CONN_ID,
                sql=f"""
                    SELECT
                        COUNT(*) AS row_count
                    FROM {BQ_CATALOG}.{BQ_SCHEMA}.{BQ_TABLE}
                """,
                autocommit=True,
                do_xcom_push=True,
            )

            inspect_bigquery_schema >> read_bigquery_table

    start >> test_trino_engine
    test_trino_engine >> [postgres_connector, bigquery_connector]

    if ENABLE_CROSS_CATALOG_TEST:
        cross_catalog_test = SQLExecuteQueryOperator(
            task_id="test_cross_catalog_query",
            conn_id=TRINO_CONN_ID,
            sql=f"""
                SELECT
                    pg_stats.postgres_row_count,
                    bq_stats.bigquery_row_count,
                    pg_stats.postgres_row_count - bq_stats.bigquery_row_count
                        AS row_count_difference
                FROM (
                    SELECT COUNT(*) AS postgres_row_count
                    FROM {PG_CATALOG}.{PG_SCHEMA}.{PG_TABLE}
                ) AS pg_stats
                CROSS JOIN (
                    SELECT COUNT(*) AS bigquery_row_count
                    FROM {BQ_CATALOG}.{BQ_SCHEMA}.{BQ_TABLE}
                ) AS bq_stats
            """,
            autocommit=True,
            do_xcom_push=True,
        )

        [postgres_connector, bigquery_connector] >> cross_catalog_test

        finish = EmptyOperator(
            task_id="connectivity_test_passed",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        cross_catalog_test >> finish
    else:
        finish = EmptyOperator(
            task_id="connectivity_test_passed",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        [postgres_connector, bigquery_connector] >> finish
