"""
DAG : postgres_to_bq_trino_dag
Airflow : 3.x
Engine  : Trino (federation layer — connects Postgres + BigQuery catalogs)

Pipeline
────────
1. [BigQueryCreateTableOperator]  Buat branches_temp di BQ via BQ API
                                  (partitioned MONTH(updated_at) + clustered, if_exists=ignore)
2. [SQLExecuteQueryOperator]      INSERT postgresql.public.branches → branches_temp via Trino
                                  (cross-catalog, window = data_interval_start s/d data_interval_end, dedup ROW_NUMBER)
3. [PythonOperator]               Schema evolution — bandingkan temp vs final,
                                  ALTER TABLE branches ADD COLUMN IF NOT EXISTS kolom baru
4. [BigQueryInsertJobOperator]    MERGE branches_temp → branches (UPSERT + guard dedup)
5. [BigQueryDeleteTableOperator]  DROP branches_temp (trigger_rule=all_done)

Kenapa BigQueryCreateTableOperator untuk step 1 (bukan Trino DDL)?
───────────────────────────────────────────────────────────────────
  Trino BigQuery connector TIDAK support WITH (partitioning=..., clustering_key=...).
  Property itu milik Iceberg/Hive connector. BigQueryCreateTableOperator pakai
  BQ REST API langsung → full support timePartitioning + clustering natively.

Kenapa Trino untuk step 2 (bukan BigQueryInsertJobOperator)?
─────────────────────────────────────────────────────────────
  Trino federasi postgresql.* → bigquery.* dalam satu query.
  Tidak butuh GCS staging, tidak butuh BQ External Connection.

Kenapa PythonOperator untuk step 3 (schema evolution)?
───────────────────────────────────────────────────────
  MERGE tidak support schemaUpdateOptions (hanya valid untuk LOAD/INSERT SELECT).
  writeDisposition dan createDisposition juga tidak valid untuk DML statements.
  Solusi: deteksi kolom baru dari temp table (cerminan Postgres hari ini),
  lalu ALTER TABLE final ADD COLUMN IF NOT EXISTS sebelum MERGE dijalankan.

Kenapa BigQueryInsertJobOperator untuk step 4 (bukan Trino)?
─────────────────────────────────────────────────────────────
  MERGE adalah BQ Standard SQL — lebih efisien dijalankan native di BQ engine.

Best practices applied
──────────────────────
  time_partitioning  — MONTH(updated_at) di temp table via table_resource
  cluster_fields     — province, branch_type, is_active
  schema_evolution   — PythonOperator ALTER TABLE sebelum MERGE (incremental, tidak full refresh)
  date_window        — {{ data_interval_start }} s/d {{ data_interval_end }} (tepat 10 menit per run)
                       manual trigger → window kosong, pakai logical_date untuk override
  dedup              — ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY updated_at DESC)
  idempotency        — if_exists=ignore (step 1), IF NOT EXISTS (step 3), job_id template (step 4)
  lineage            — outlets = Asset(...) di setiap step
  cleanup            — trigger_rule=all_done pada DROP temp (jalan meski MERGE gagal)

Catatan penting
───────────────
  writeDisposition, createDisposition, schemaUpdateOptions — TIDAK dipakai di MERGE.
  Ketiganya hanya valid untuk job dengan destinationTable (LOAD / INSERT SELECT).
  Menggunakannya pada DML statement akan menyebabkan error BQ 400.

Connections (Airflow UI → Admin → Connections)
───────────────────────────────────────────────
  trino_default        : Trino  (Conn Type=trino, host=banking-trino, port=8080, login=trino)
  google_cloud_default : GCP service account dengan role BigQuery Data Editor + Job User
"""

from __future__ import annotations
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.sdk.definitions.asset import Asset
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param

from helpers.trino_helper import (
    build_bq_merge_query,
    build_metadata_exprs,
    build_table_resource,
    build_trino_columns,
    build_trino_insert_sql,
    parse_columns,
    sync_final_table_schema,
)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG  ← ubah bagian ini saat duplikat untuk tabel lain
# ══════════════════════════════════════════════════════════════════════════════

DAG_ID          = "postgres_to_bq_trino_customers_v3_w_helper"
SCHEDULE        = "0 9 * * *"                   # setiap jam 09:00 WIB
START_DATE      = pendulum.datetime(2022, 6, 3, tz="Asia/Jakarta")
SOURCE_TZ       = "Asia/Jakarta"                 # timezone kolom sumber di Postgres

TRINO_CONN_ID   = "trino_default"
GCP_CONN_ID     = "google_cloud_default"

TRINO_BQ_CAT    = "bigquery"                     # Trino catalog → bigquery.properties
TRINO_PG_CAT    = "postgresql"                   # Trino catalog → postgresql.properties

BQ_PROJECT      = "taxi-pipeline-123"
BQ_DATASET      = "dev_bronze_pg"
BQ_LOCATION     = "US"

PG_SCHEMA       = "public"
PG_TABLE        = "customers"
BQ_FINAL_TABLE  = "customers"
BQ_TEMP_TABLE   = BQ_FINAL_TABLE + "_temp_{{ ds_nodash }}"

# Bronze DDL   → PARTITION BY DATE(created_at)    ← immutable, storage layout
# Airflow DAG  → PARTITION_FIELD = "updated_at"   ← window filter & MERGE condition
PARTITION_FIELD = "updated_at"                   # kolom TIMESTAMP / DATE untuk partisi
PARTITION_TYPE  = "MONTH"                        # DAY | MONTH | YEAR
CLUSTER_FIELDS  = ["gender"]  # max 4 kolom
MERGE_KEY       = "customer_id"                  # primary key untuk UPSERT
SOURCE_SYSTEM   = "ride_ops_pg"                  # nilai literal kolom _source_system

# True  → tabel hanya INSERT, tidak pernah di-UPDATE (contoh: trip_status_logs)
#          WHEN MATCHED → UPDATE tidak akan dibuat di SQL MERGE
# False → tabel bisa di-UPDATE (contoh: customers, trips, payments)
#          WHEN MATCHED → UPDATE tetap ada, hanya jika data lebih baru
APPEND_ONLY     = False

# ══════════════════════════════════════════════════════════════════════════════
#  SCHEMA  ← ubah ini saat duplikat (tambah / hapus kolom sesuai tabel baru)
#  Metadata _ingested_at & _source_system selalu ada — jangan dihapus.
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA_FIELDS = [
    {"name": "customer_id",     "type": "STRING",    "mode": "REQUIRED"},
    {"name": "full_name",       "type": "STRING",    "mode": "REQUIRED"},
    {"name": "phone_number",    "type": "STRING",    "mode": "NULLABLE"},
    {"name": "email",           "type": "STRING",    "mode": "NULLABLE"},
    {"name": "gender",          "type": "STRING",    "mode": "NULLABLE"},
    {"name": "birth_date",      "type": "DATE",      "mode": "NULLABLE"},
    {"name": "created_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "updated_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
    # pipeline metadata — selalu ada, jangan dihapus
    {"name": "_ingested_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "_source_system",  "type": "STRING",    "mode": "REQUIRED"},
]

# ══════════════════════════════════════════════════════════════════════════════
#  TABLE_COLUMNS  ← kolom yang berasal dari Postgres (tanpa _ingested_at & _source_system)
#  Cara dapat: jalankan `\d <table>` di psql, copy nama kolom → paste di sini.
# ══════════════════════════════════════════════════════════════════════════════

TABLE_COLUMNS = """
    customer_id, full_name, phone_number, email,
    gender, birth_date, created_at, updated_at
"""

# ══════════════════════════════════════════════════════════════════════════════
#  AUTO-DERIVE  ← tidak perlu diubah
# ══════════════════════════════════════════════════════════════════════════════

_schema_lookup  = {f["name"]: f["type"] for f in SCHEMA_FIELDS}
_columns        = parse_columns(TABLE_COLUMNS, _schema_lookup)
_trino_columns  = build_trino_columns(_columns, _schema_lookup)
_metadata_exprs = build_metadata_exprs(SOURCE_SYSTEM)

BQ_FINAL_ASSET  = Asset(f"bigquery://{BQ_PROJECT}/{BQ_DATASET}/{BQ_FINAL_TABLE}")
BQ_TEMP_ASSET   = Asset(f"bigquery://{BQ_PROJECT}/{BQ_DATASET}/{BQ_TEMP_TABLE}")

# ══════════════════════════════════════════════════════════════════════════════
#  DAG
# ══════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner":                     "data-engineering",
    "depends_on_past":           False,
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    description=f"Postgres → BigQuery via Trino | {PG_TABLE} → {BQ_DATASET}.{BQ_FINAL_TABLE}",
    default_args=default_args,
    schedule="@once",
    start_date=START_DATE,
    catchup=True,
    max_active_runs=1,
    tags=["postgres", "bigquery", "trino", "ingestion"],
    doc_md=__doc__,
    params={
        # Isi saat manual trigger untuk override window data_interval_start/end.
        # Kosongkan (null) untuk pakai window otomatis dari schedule.
        # Format: "YYYY-MM-DD HH:MM:SS" dalam timezone SOURCE_TZ (Asia/Jakarta).
        "window_start": Param(
            default=None,
            type=["null", "string"],
            description=f"Window start (inklusif) dalam {SOURCE_TZ}. "
                        f"Contoh: 2026-03-11 09:00:00",
        ),
        "window_end": Param(
            default=None,
            type=["null", "string"],
            description=f"Window end (eksklusif) dalam {SOURCE_TZ}. "
                        f"Contoh: 2026-03-12 09:00:00",
        ),
    },
) as dag:

    # ── Step 1: CREATE temp table via BigQuery API ───────────────────────────
    create_bq_temp = BigQueryCreateTableOperator(
        task_id="create_bq_temp_table",
        gcp_conn_id=GCP_CONN_ID,
        project_id=BQ_PROJECT,
        dataset_id=BQ_DATASET,
        table_id=BQ_TEMP_TABLE,
        table_resource=build_table_resource(
            bq_project=BQ_PROJECT,
            bq_dataset=BQ_DATASET,
            table_id=BQ_TEMP_TABLE,
            schema_fields=SCHEMA_FIELDS,
            partition_field=PARTITION_FIELD,
            partition_type=PARTITION_TYPE,
            cluster_fields=CLUSTER_FIELDS,
        ),
        if_exists="ignore",
        outlets=[BQ_TEMP_ASSET],
    )

    # ── Step 2: INSERT Postgres → temp via Trino cross-catalog ──────────────
    insert_to_bq_temp = SQLExecuteQueryOperator(
        task_id="insert_postgres_to_bq_temp",
        conn_id=TRINO_CONN_ID,
        sql=build_trino_insert_sql(
            trino_bq_catalog=TRINO_BQ_CAT,
            trino_pg_catalog=TRINO_PG_CAT,
            bq_dataset=BQ_DATASET,
            bq_temp_table=BQ_TEMP_TABLE,
            pg_schema=PG_SCHEMA,
            pg_source_table=PG_TABLE,
            merge_key=MERGE_KEY,
            partition_field=PARTITION_FIELD,
            columns=_columns,
            trino_columns=_trino_columns,
            metadata_exprs=_metadata_exprs,
            source_tz=SOURCE_TZ,
        ),
        autocommit=True,
        outlets=[BQ_TEMP_ASSET],
    )

    # ── Step 3: Schema evolution — ADD COLUMN IF NOT EXISTS ─────────────────
    sync_schema = PythonOperator(
        task_id="sync_final_table_schema",
        python_callable=sync_final_table_schema,
        op_kwargs={
            "gcp_conn_id":    GCP_CONN_ID,
            "bq_project":     BQ_PROJECT,
            "bq_dataset":     BQ_DATASET,
            "bq_final_table": BQ_FINAL_TABLE,
        },
    )

    # ── Step 4: MERGE temp → final (UPSERT) ─────────────────────────────────
    merge_to_final = BigQueryInsertJobOperator(
        task_id="merge_temp_to_final",
        gcp_conn_id=GCP_CONN_ID,
        project_id=BQ_PROJECT,
        location=BQ_LOCATION,
        configuration=build_bq_merge_query(
            bq_project=BQ_PROJECT,
            bq_dataset=BQ_DATASET,
            bq_final_table=BQ_FINAL_TABLE,
            bq_temp_table=BQ_TEMP_TABLE,
            merge_key=MERGE_KEY,
            partition_field=PARTITION_FIELD,
            columns=_columns,
            append_only=APPEND_ONLY,
        ),
        job_id="{{ dag.dag_id }}__merge__{{ ds_nodash }}",
        force_rerun=True,
        deferrable=False,
        outlets=[BQ_FINAL_ASSET],
    )

    # ── Step 5: DROP temp (selalu jalan meski step sebelumnya gagal) ─────────
    drop_bq_temp = BigQueryDeleteTableOperator(
        task_id="drop_bq_temp_table",
        gcp_conn_id=GCP_CONN_ID,
        deletion_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TEMP_TABLE}",
        ignore_if_missing=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_bq_temp >> insert_to_bq_temp >> sync_schema >> merge_to_final >> drop_bq_temp