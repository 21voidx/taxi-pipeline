"""
DAG : postgres_to_bq_trino_multi_table
Airflow : 3.x
Engine  : Trino (federation layer — connects Postgres + BigQuery catalogs)

Pipeline per tabel (dalam TaskGroup paralel)
────────────────────────────────────────────
1. [BigQueryCreateTableOperator]  Buat <table>_temp di BQ via BQ API
2. [SQLExecuteQueryOperator]      INSERT postgresql.<schema>.<table> → <table>_temp via Trino
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

DAG_ID        = "postgres_to_bq_trino_multi_table_V2"
SOURCE_TZ     = "Asia/Jakarta"

TRINO_CONN_ID = "trino_default"
GCP_CONN_ID   = "google_cloud_default"
TRINO_BQ_CAT  = "bigquery"
TRINO_PG_CAT  = "postgresql"

BQ_PROJECT    = "taxi-pipeline-123"
BQ_DATASET    = "dev_bronze_pg"
BQ_LOCATION   = "US"
PG_SCHEMA     = "public"

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

    # ── customers ──────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "customers",
        bq_final_table  = "customers",
        merge_key       = "customer_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["gender"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "customer_id",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "full_name",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "phone_number",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "email",          "type": "STRING",    "mode": "NULLABLE"},
            {"name": "gender",         "type": "STRING",    "mode": "NULLABLE"},
            {"name": "birth_date",     "type": "DATE",      "mode": "NULLABLE"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            customer_id, full_name, phone_number, email,
            gender, birth_date, created_at, updated_at
        """,
    ),
    # ── driver ──────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "drivers",
        bq_final_table  = "drivers",
        merge_key       = "driver_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["city", "driver_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "driver_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "full_name",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "phone_number",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "email",           "type": "STRING",    "mode": "NULLABLE"},
            {"name": "license_number",  "type": "STRING",    "mode": "NULLABLE"},
            {"name": "driver_status",   "type": "STRING",    "mode": "REQUIRED"},
            {"name": "join_date",       "type": "DATE",      "mode": "NULLABLE"},
            {"name": "city",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            # pipeline metadata — selalu ada, jangan dihapus
            {"name": "_ingested_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",  "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            driver_id, full_name, phone_number, email,
            license_number, driver_status, join_date, city,
            created_at, updated_at
        """,
    ),
    # ── vehicles ──────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "vehicles",
        bq_final_table  = "vehicles",
        merge_key       = "vehicle_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["vehicle_type", "driver_id"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "vehicle_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "driver_id",        "type": "STRING",    "mode": "REQUIRED"},
            {"name": "plate_number",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "vehicle_type",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "brand",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "model",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "production_year",  "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "seat_capacity",    "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            # pipeline metadata — selalu ada, jangan dihapus
            {"name": "_ingested_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",   "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            vehicle_id, driver_id, plate_number, vehicle_type,
            brand, model, production_year, seat_capacity,
            created_at, updated_at
        """,
    ),
    # ── ratings ──────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "ratings",
        bq_final_table  = "ratings",
        merge_key       = "rating_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["driver_id", "customer_id", "rating_score"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "rating_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "trip_id",         "type": "STRING",    "mode": "REQUIRED"},
            {"name": "customer_id",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "driver_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "rating_score",    "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "review_text",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "rated_ts",        "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            # pipeline metadata — selalu ada, jangan dihapus
            {"name": "_ingested_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",  "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            rating_id, trip_id, customer_id, driver_id,
            rating_score, review_text, rated_ts,
            created_at, updated_at
        """,
    ),
    # ── driver_payouts ──────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "driver_payouts",
        bq_final_table  = "driver_payouts",
        merge_key       = "payout_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["driver_id", "payout_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "payout_id",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "driver_id",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "trip_id",              "type": "STRING",    "mode": "REQUIRED"},
            {"name": "payout_date",          "type": "DATE",      "mode": "NULLABLE"},
            {"name": "base_earning",         "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "incentive_amount",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "bonus_amount",         "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "deduction_amount",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "final_payout_amount",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "payout_status",        "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            # pipeline metadata — selalu ada, jangan dihapus
            {"name": "_ingested_at",         "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",       "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            payout_id, driver_id, trip_id, payout_date,
            base_earning, incentive_amount, bonus_amount,
            deduction_amount, final_payout_amount, payout_status,
            created_at, updated_at
        """,
    ),
    # ── payments ───────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "payments",
        bq_final_table  = "payments",
        merge_key       = "payment_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["payment_method", "payment_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "payment_id",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "trip_id",         "type": "STRING",    "mode": "REQUIRED"},
            {"name": "customer_id",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "payment_method",  "type": "STRING",    "mode": "NULLABLE"},
            {"name": "payment_status",  "type": "STRING",    "mode": "NULLABLE"},
            {"name": "gross_amount",    "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "discount_amount", "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "tax_amount",      "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "toll_amount",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "tip_amount",      "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "net_amount",      "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "paid_ts",         "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            # pipeline metadata — selalu ada, jangan dihapus
            {"name": "_ingested_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",  "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            payment_id, trip_id, customer_id,
            payment_method, payment_status,
            gross_amount, discount_amount, tax_amount,
            toll_amount, tip_amount, net_amount,
            paid_ts, created_at, updated_at
        """,
    ),
    # ── trip_status_logs ───────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "trip_status_logs",
        bq_final_table  = "trip_status_logs",
        merge_key       = "trip_status_log_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["status_code", "actor_type"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "trip_status_log_id", "type": "STRING",    "mode": "REQUIRED"},
            {"name": "trip_id",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "status_code",        "type": "STRING",    "mode": "REQUIRED"},
            {"name": "status_ts",          "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "actor_type",         "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",         "type": "TIMESTAMP", "mode": "REQUIRED"},
            # pipeline metadata — selalu ada, jangan dihapus
            {"name": "_ingested_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",     "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            trip_status_log_id, trip_id, status_code,
            status_ts, actor_type, created_at
        """,
    ),

    # ── trips ──────────────────────────────────────────────────────────────────
    TableConfig(
        pg_table        = "trips",
        bq_final_table  = "trips",
        merge_key       = "trip_id",
        partition_field = "request_ts",
        partition_type  = "MONTH",
        cluster_fields  = ["city", "trip_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "trip_id",                "type": "STRING",    "mode": "REQUIRED"},
            {"name": "customer_id",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "driver_id",              "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_id",             "type": "STRING",    "mode": "REQUIRED"},
            {"name": "request_ts",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "pickup_ts",              "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "dropoff_ts",             "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "pickup_lat",             "type": "FLOAT64",   "mode": "NULLABLE"},
            {"name": "pickup_lng",             "type": "FLOAT64",   "mode": "NULLABLE"},
            {"name": "dropoff_lat",            "type": "FLOAT64",   "mode": "NULLABLE"},
            {"name": "dropoff_lng",            "type": "FLOAT64",   "mode": "NULLABLE"},
            {"name": "pickup_area",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "dropoff_area",           "type": "STRING",    "mode": "NULLABLE"},
            {"name": "city",                   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "estimated_distance_km",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "actual_distance_km",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "estimated_fare",         "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "actual_fare",            "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "surge_multiplier",       "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "trip_status",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "cancel_reason",          "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",         "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            trip_id, customer_id, driver_id, vehicle_id,
            request_ts, pickup_ts, dropoff_ts,
            pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
            pickup_area, dropoff_area, city,
            estimated_distance_km, actual_distance_km,
            estimated_fare, actual_fare, surge_multiplier,
            trip_status, cancel_reason,
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
        f"Postgres → BigQuery via Trino | "
        f"multi-table ingestion ke {BQ_DATASET} "
        f"({len(TABLE_CONFIGS)} tabel)"
    ),
    default_args=default_args,
    schedule=CronDataIntervalTimetable("0 9 * * *", timezone="Asia/Jakarta"),
    start_date=pendulum.datetime(2022, 6, 3, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "bigquery", "trino", "ingestion", "multi-table"],
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