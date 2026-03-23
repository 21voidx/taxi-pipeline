"""
DAG : mysql_to_bq_trino_multi_table
Airflow : 3.x
Engine  : Trino (federation layer — connects Mysql + BigQuery catalogs)

Pipeline per tabel (dalam TaskGroup paralel)
────────────────────────────────────────────
1. [BigQueryCreateTableOperator]  Buat <table>_temp di BQ via BQ API
2. [SQLExecuteQueryOperator]      INSERT mysql.<schema>.<table> → <table>_temp via Trino
3. [PythonOperator]               Schema evolution — ALTER TABLE ADD COLUMN IF NOT EXISTS
4. [BigQueryInsertJobOperator]    MERGE <table>_temp → <table> (UPSERT)
5. [BigQueryDeleteTableOperator]  DROP <table>_temp (trigger_rule=all_done)

Cara menambah / menghapus tabel
────────────────────────────────
Cukup tambah / hapus entry di TABLE_CONFIGS.
DAG otomatis membuat 1 TaskGroup per tabel, semua berjalan paralel.
"""

from __future__ import annotations
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.timetables.interval import CronDataIntervalTimetable
from helpers.trino_helper_task_group import TableConfig, make_table_task_group

# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DAG_ID        = "Mysql_to_bq_trino_multi_table"
SOURCE_TZ     = "Asia/Jakarta"

TRINO_CONN_ID = "trino_default"
GCP_CONN_ID   = "google_cloud_default"
TRINO_BQ_CAT  = "bigquery"
TRINO_PG_CAT  = "mysql"

BQ_PROJECT    = "taxi-pipeline-123"
BQ_DATASET    = "dev_bronze_mysql"
BQ_LOCATION   = "US"
PG_SCHEMA     = "ride_marketing_mysql"

# ── Shared kwargs diteruskan ke setiap make_table_task_group() ────────────────
# Definisikan sekali di sini agar tidak repeat di setiap entry TABLE_CONFIGS.
SHARED = dict(
    bq_project    = BQ_PROJECT,
    bq_dataset    = BQ_DATASET,
    bq_location   = BQ_LOCATION,
    pg_schema     = PG_SCHEMA,
    trino_conn_id = TRINO_CONN_ID,
    gcp_conn_id   = GCP_CONN_ID,
    trino_bq_cat  = TRINO_BQ_CAT,
    trino_pg_cat  = TRINO_PG_CAT,
    source_tz     = SOURCE_TZ,
)

# ══════════════════════════════════════════════════════════════════════════════
#  TABLE_CONFIGS  ← tambah / hapus entry di sini
# ══════════════════════════════════════════════════════════════════════════════

TABLE_CONFIGS: list[TableConfig] = [

    # ── promotions ──────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "promotions",
        bq_final_table  = "promotions",
        merge_key       = "promo_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["promo_type", "promo_status"],
        source_system   = "ride_marketing_mysql",
        append_only     = False,
        schema_fields   = [
            {"name": "promo_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "promo_code",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "promo_name",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "promo_type",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "discount_type",  "type": "STRING",    "mode": "NULLABLE"},
            {"name": "discount_value", "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "start_date",     "type": "DATE",      "mode": "NULLABLE"},
            {"name": "end_date",       "type": "DATE",      "mode": "NULLABLE"},
            {"name": "budget_amount",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "promo_status",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            promo_id, promo_code, promo_name, promo_type,
            discount_type, discount_value, start_date, end_date,
            budget_amount, promo_status,
            created_at, updated_at
        """,
    ),
    # ── promo_redemptions ──────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "promo_redemptions",
        bq_final_table  = "promo_redemptions",
        merge_key       = "redemption_id",
        partition_field = "redeemed_ts",
        partition_type  = "DAY",
        cluster_fields  = ["promo_id", "redemption_status"],
        source_system   = "ride_marketing_mysql",
        append_only     = False,
        schema_fields   = [
            {"name": "redemption_id",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "promo_id",          "type": "STRING",    "mode": "REQUIRED"},
            {"name": "customer_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "trip_id",           "type": "STRING",    "mode": "REQUIRED"},
            {"name": "redeemed_ts",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "discount_amount",   "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "redemption_status", "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",    "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            redemption_id, promo_id, customer_id, trip_id,
            redeemed_ts, discount_amount, redemption_status,
            created_at, updated_at
        """,
    ),
 
    # ── customer_segments ──────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "customer_segments",
        bq_final_table  = "customer_segments",
        merge_key       = "id",
        partition_field = "snapshot_date",
        partition_type  = "MONTH",
        cluster_fields  = ["segment_name", "customer_id"],
        source_system   = "ride_marketing_mysql",
        append_only     = False,
        schema_fields   = [
            {"name": "id",             "type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "customer_id",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "segment_name",   "type": "STRING",    "mode": "REQUIRED"},
            {"name": "segment_score",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "snapshot_date",  "type": "DATE",      "mode": "REQUIRED"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            id, customer_id, segment_name, segment_score,
            snapshot_date, created_at, updated_at
        """,
    ),
 
    # ── campaign_spend ─────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "campaign_spend",
        bq_final_table  = "campaign_spend",
        merge_key       = "campaign_id",
        partition_field = "spend_date",
        partition_type  = "MONTH",
        cluster_fields  = ["channel_name"],
        source_system   = "ride_marketing_mysql",
        append_only     = False,
        schema_fields   = [
            {"name": "campaign_id",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "campaign_name",  "type": "STRING",    "mode": "NULLABLE"},
            {"name": "channel_name",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "spend_date",     "type": "DATE",      "mode": "NULLABLE"},
            {"name": "spend_amount",   "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "impressions",    "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "clicks",         "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "installs",       "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            campaign_id, campaign_name, channel_name, spend_date,
            spend_amount, impressions, clicks, installs,
            created_at, updated_at
        """,
    ),


]

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
    description=(
        f"Mysql → BigQuery via Trino | "
        f"multi-table ingestion ke {BQ_DATASET} "
        f"({len(TABLE_CONFIGS)} tabel)"
    ),
    default_args=default_args,
    schedule=CronDataIntervalTimetable("0 9 * * *", timezone="Asia/Jakarta"),
    start_date=pendulum.datetime(2022, 6, 3, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["mysql", "bigquery", "trino", "ingestion", "multi-table"],
    doc_md=__doc__,
    params={
        "window_start": Param(
            default=None,
            type=["null", "string"],
            description=f"Window start (inklusif) dalam {SOURCE_TZ}. Contoh: 2026-03-11 09:00:00",
        ),
        "window_end": Param(
            default=None,
            type=["null", "string"],
            description=f"Window end (eksklusif) dalam {SOURCE_TZ}. Contoh: 2026-03-12 09:00:00",
        ),
    },
) as dag:

    # Buat satu TaskGroup per tabel — semua berjalan paralel
    table_groups = [make_table_task_group(cfg, **SHARED) for cfg in TABLE_CONFIGS]

    # ── Opsional: tambahkan dependency antar group jika urutan penting ─────────
    # Contoh urutan sequential:  table_groups[0] >> table_groups[1]
    # Default (tidak ada >>)  :  semua TaskGroup berjalan paralel