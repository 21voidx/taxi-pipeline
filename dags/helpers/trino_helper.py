"""
helpers/pg_to_bq_trino_helper.py
═══════════════════════════════════════════════════════════════════════════════
Reusable utilities untuk pipeline Postgres → BigQuery via Trino cross-catalog.

Cara pakai di DAG:
    from helpers.pg_to_bq_trino_helper import (
        parse_columns,
        build_trino_columns,
        build_trino_insert_sql,
        build_table_resource,
        build_bq_merge_query,
        sync_final_table_schema,
        METADATA_NAMES,
        METADATA_EXPRS,
        BQ_TO_TRINO_CAST,
    )

Semua fungsi menerima parameter eksplisit (tidak membaca global DAG) sehingga
bisa dipanggil dari DAG manapun tanpa konflik state.
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations
import logging


# ─────────────────────────────────────────────────────────────────────────────
#  TYPE MAP  — Trino type mismatch antara PG connector dan BQ connector
# ─────────────────────────────────────────────────────────────────────────────
#
#   BQ type  │ Trino(BQ) │ PG type contoh            │ Trino(PG)      │ CAST ke
#   ─────────────────────────────────────────────────────────────────────────
#   INTEGER  │ bigint    │ SERIAL, INT4, INT          │ integer        │ BIGINT
#   STRING   │ varchar   │ VARCHAR(N), CHAR(N), UUID  │ varchar(N)/uuid│ VARCHAR
#   DATE     │ date      │ DATE                       │ date           │ — ✓
#   TIMESTAMP│ timestamp │ TIMESTAMPTZ                │ timestamp tz   │ — ✓
#   BOOLEAN  │ boolean   │ BOOLEAN                    │ boolean        │ — ✓
#   FLOAT    │ double    │ FLOAT8                     │ double         │ — ✓
#   NUMERIC  │ decimal   │ NUMERIC                    │ decimal        │ — ✓

BQ_TO_TRINO_CAST: dict[str, str] = {
    "INTEGER": "BIGINT",  # BQ INT64  ↔ PG INT4/SERIAL
    "STRING":  "VARCHAR", # BQ STRING ↔ PG VARCHAR/CHAR/UUID
}

# ─────────────────────────────────────────────────────────────────────────────
#  PIPELINE METADATA COLUMNS
# ─────────────────────────────────────────────────────────────────────────────
#  Kolom ini TIDAK berasal dari Postgres → tidak masuk TABLE_COLUMNS.
#  Diisi sebagai SQL literal di Trino SELECT, injected di tiga titik berbeda:
#    a. INSERT target list  → METADATA_NAMES
#    b. SELECT expressions  → METADATA_EXPRS  (literal SQL)
#    c. SELECT final CTE    → METADATA_NAMES
#  dan juga di MERGE set_clause + ins_cols/ins_vals.

METADATA_NAMES: list[str] = ["_ingested_at", "_source_system"]


def build_metadata_exprs(source_system: str) -> list[str]:
    """
    Generate literal SQL expressions untuk kolom metadata.
    Dipanggil saat build SQL agar source_system bisa berbeda per DAG.

        source_system : identifier sistem sumber, contoh 'ride_ops_pg'
    """
    return [
        "CURRENT_TIMESTAMP        AS _ingested_at",
        f"'{source_system}'       AS _source_system",
    ]


# ─────────────────────────────────────────────────────────────────────────────
#  COLUMN PARSING & CAST BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def parse_columns(table_columns_str: str, schema_lookup: dict[str, str]) -> list[str]:
    """
    Parse TABLE_COLUMNS string → list nama kolom bersih.

    Fail-fast: raise ValueError jika ada nama kolom yang tidak ada di
    SCHEMA_FIELDS, sehingga typo terdeteksi sebelum DAG berjalan.

        table_columns_str : multi-line string nama kolom, pisah koma
        schema_lookup     : dict {col_name: bq_type} dari SCHEMA_FIELDS
    """
    columns: list[str] = []
    for col in table_columns_str.replace("\n", ",").split(","):
        col = col.strip()
        if not col:
            continue
        if col not in schema_lookup:
            raise ValueError(
                f"Kolom '{col}' tidak ditemukan di SCHEMA_FIELDS. "
                f"Periksa typo atau tambahkan kolom ke SCHEMA_FIELDS terlebih dahulu."
            )
        columns.append(col)
    return columns


def build_trino_columns(columns: list[str], schema_lookup: dict[str, str]) -> list[str]:
    """
    Build list Trino SELECT expressions dengan CAST yang tepat.

    Kolom bertipe INTEGER atau STRING di BQ perlu di-CAST eksplisit karena
    Trino memetakan tipe PG dan BQ secara berbeda (lihat BQ_TO_TRINO_CAST).
    Tipe lain (DATE, TIMESTAMP, BOOLEAN, FLOAT, NUMERIC) sudah cocok → tidak perlu CAST.

        columns       : list nama kolom dari parse_columns()
        schema_lookup : dict {col_name: bq_type} dari SCHEMA_FIELDS
    """
    return [
        (
            f"CAST(src.{col} AS {BQ_TO_TRINO_CAST[schema_lookup[col]]}) AS {col}"
            if schema_lookup[col] in BQ_TO_TRINO_CAST
            else f"src.{col}"
        )
        for col in columns
    ]


# ─────────────────────────────────────────────────────────────────────────────
#  SQL BUILDER — Trino INSERT
# ─────────────────────────────────────────────────────────────────────────────

def build_trino_insert_sql(
    *,
    trino_bq_catalog : str,
    trino_pg_catalog : str,
    bq_dataset       : str,
    bq_temp_table    : str,
    pg_schema        : str,
    pg_source_table  : str,
    merge_key        : str,
    partition_field  : str,
    columns          : list[str],
    trino_columns    : list[str],
    metadata_exprs   : list[str],
    source_tz        : str = "Asia/Jakarta",
) -> str:
    """
    Build Trino cross-catalog INSERT SQL dengan:
      • CTE ranked   — dedup ROW_NUMBER() per merge_key, ambil partition_field terbaru
      • Window filter — data_interval_start/end dikonversi ke source_tz
      • Metadata      — CURRENT_TIMESTAMP + source_system literal diinjeksi di SELECT

    Kenapa timezone dikonversi?
    ───────────────────────────
    Airflow menyimpan data_interval dalam UTC. Jika kolom partition_field di Postgres
    disimpan dalam WIB (+0700), literal UTC akan menggeser window dan melewatkan data.
    Konversi ke source_tz memastikan window cocok dengan timezone kolom sumber.

    Override window via dag_run.conf (nilai dalam source_tz):
        { "window_start": "2026-03-11 09:00:00", "window_end": "2026-03-12 09:00:00" }
    """
    all_names  = columns + METADATA_NAMES
    all_exprs  = trino_columns + metadata_exprs

    insert_cols = ",\n        ".join(all_names)
    trino_exprs = ",\n            ".join(all_exprs)
    final_cols  = ", ".join(all_names)

    return f"""
INSERT INTO {trino_bq_catalog}.{bq_dataset}.{bq_temp_table} (
    {insert_cols}
)
WITH ranked AS (
    SELECT
        {trino_exprs},
        ROW_NUMBER() OVER (
            PARTITION BY src.{merge_key}
            ORDER BY src.{partition_field} DESC
        ) AS _rn
    FROM {trino_pg_catalog}.{pg_schema}.{pg_source_table} AS src
    WHERE src.{partition_field} >= TIMESTAMP '{{{{ dag_run.conf.get("window_start") or data_interval_start.in_timezone("{source_tz}").strftime("%Y-%m-%d %H:%M:%S") }}}}'
      AND src.{partition_field} <  TIMESTAMP '{{{{ dag_run.conf.get("window_end")   or data_interval_end.in_timezone("{source_tz}").strftime("%Y-%m-%d %H:%M:%S") }}}}'
)
SELECT {final_cols}
FROM   ranked
WHERE  _rn = 1
"""


# ─────────────────────────────────────────────────────────────────────────────
#  TABLE RESOURCE BUILDER — BigQuery REST API format
# ─────────────────────────────────────────────────────────────────────────────

def build_table_resource(
    *,
    bq_project      : str,
    bq_dataset      : str,
    table_id        : str,
    schema_fields   : list[dict],
    partition_field : str,
    partition_type  : str = "MONTH",
    cluster_fields  : list[str] | None = None,
) -> dict:
    """
    Build BQ table_resource dict untuk BigQueryCreateTableOperator.

    Kenapa tidak pakai Trino DDL?
    ──────────────────────────────
    Trino BigQuery connector tidak support WITH (partitioning=..., clustering_key=...).
    Property itu milik Iceberg/Hive connector. BQ REST API (via table_resource) adalah
    satu-satunya cara set timePartitioning + clustering secara programatik dari Airflow.

        partition_type  : 'DAY' | 'MONTH' | 'YEAR'  (default: 'MONTH')
        cluster_fields  : list kolom untuk clustering, max 4
    """
    resource: dict = {
        "tableReference": {
            "projectId": bq_project,
            "datasetId": bq_dataset,
            "tableId":   table_id,
        },
        "schema": {
            "fields": schema_fields,
        },
        "timePartitioning": {
            "type":  partition_type,
            "field": partition_field,
        },
    }
    if cluster_fields:
        resource["clustering"] = {"fields": cluster_fields}
    return resource


# ─────────────────────────────────────────────────────────────────────────────
#  SQL BUILDER — BigQuery MERGE
# ─────────────────────────────────────────────────────────────────────────────

def build_bq_merge_query(
    *,
    bq_project      : str,
    bq_dataset      : str,
    bq_final_table  : str,
    bq_temp_table   : str,
    merge_key       : str,
    partition_field : str,
    columns         : list[str],
    append_only     : bool = False,
) -> dict:
    """
    Build BigQuery MERGE job configuration dict untuk BigQueryInsertJobOperator.

    append_only = False  (default) — tabel bisa di-UPDATE (customers, trips, payments)
      • WHEN MATCHED AND incoming lebih baru → UPDATE SET semua kolom non-key
      • WHEN NOT MATCHED                     → INSERT baris baru

    append_only = True — tabel hanya INSERT, tidak pernah di-UPDATE (trip_status_logs, dll)
      • WHEN MATCHED                         → tidak ada clause (skip UPDATE)
      • WHEN NOT MATCHED                     → INSERT baris baru
      Cocok untuk event log / audit trail yang immutable by design.

    ROW_NUMBER() guard di dalam MERGE USING → safety net dedup di kedua mode.

    Catatan:
      writeDisposition, createDisposition, schemaUpdateOptions TIDAK dipakai —
      ketiganya tidak valid untuk DML statement dan akan menyebabkan BQ error 400.
    """
    all_cols = columns + METADATA_NAMES
    ins_cols = ", ".join(all_cols)
    ins_vals = ", ".join(f"S.{c}" for c in all_cols)

    if append_only:
        # Tabel immutable — WHEN MATCHED tidak dibuat sama sekali.
        # Baris yang sudah ada di target dibiarkan, hanya baris baru yang di-INSERT.
        when_matched_clause = ""
    else:
        # Tabel mutable — UPDATE hanya jika data yang masuk lebih baru dari yang ada.
        # Mencegah overwrite data yang sudah lebih update (out-of-order event).
        non_key_cols = [c for c in columns if c != merge_key]
        all_non_key  = non_key_cols + METADATA_NAMES  # metadata ikut di-refresh saat UPDATE
        set_clause   = ",\n                        ".join(
            f"T.{c} = S.{c}" for c in all_non_key
        )
        when_matched_clause = f"""
                WHEN MATCHED AND S.{partition_field} > T.{partition_field} THEN
                    UPDATE SET
                        {set_clause}
"""

    return {
        "query": {
            "query": f"""
                MERGE `{bq_project}.{bq_dataset}.{bq_final_table}` AS T
                USING (
                    SELECT * EXCEPT(_rn)
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY {merge_key}
                                ORDER BY {partition_field} DESC
                            ) AS _rn
                        FROM `{bq_project}.{bq_dataset}.{bq_temp_table}`
                    )
                    WHERE _rn = 1
                ) AS S
                ON T.{merge_key} = S.{merge_key}
{when_matched_clause}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({ins_cols})
                    VALUES ({ins_vals})

                -- WHEN NOT MATCHED BY SOURCE THEN DELETE  ← aktifkan jika butuh soft-delete
            """,
            "useLegacySql": False,
            "defaultDataset": {
                "projectId": bq_project,
                "datasetId": bq_dataset,
            },
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
#  SCHEMA EVOLUTION — callable untuk PythonOperator
# ─────────────────────────────────────────────────────────────────────────────

def sync_final_table_schema(
    ds_nodash       : str,
    gcp_conn_id     : str,
    bq_project      : str,
    bq_dataset      : str,
    bq_final_table  : str,
    **kwargs,
) -> str:
    """
    Schema evolution: deteksi kolom baru di temp table, ALTER TABLE final untuk menambahnya.

    Dipanggil via PythonOperator dengan op_kwargs berisi semua parameter di atas.
    ds_nodash diterima otomatis dari Airflow context injection.

    Kenapa PythonOperator dan bukan di dalam MERGE?
    ────────────────────────────────────────────────
    MERGE tidak support schemaUpdateOptions (hanya valid untuk LOAD/INSERT SELECT).
    Solusi: deteksi kolom baru dari temp table (cerminan Postgres hari ini),
    lalu ALTER TABLE final ADD COLUMN IF NOT EXISTS sebelum MERGE dijalankan.

    Hanya menangani ADD COLUMN (safe evolution).
    DROP COLUMN / type change memerlukan DBA sign-off sesuai prosedur compliance.

    Idempotent: IF NOT EXISTS → aman di-retry tanpa error "duplicate column".
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook   = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client(project_id=bq_project)

    temp_ref  = f"{bq_project}.{bq_dataset}.{bq_final_table}_temp_{ds_nodash}"
    final_ref = f"{bq_project}.{bq_dataset}.{bq_final_table}"

    try:
        temp_table  = client.get_table(temp_ref)
        final_table = client.get_table(final_ref)
    except Exception as exc:
        raise RuntimeError(
            f"Gagal mengambil schema tabel. "
            f"Pastikan '{temp_ref}' (step 2) dan '{final_ref}' sudah ada. "
            f"Detail: {exc}"
        ) from exc

    temp_fields  = {f.name: f for f in temp_table.schema}
    final_fields = {f.name for f in final_table.schema}
    new_cols     = [f for name, f in temp_fields.items() if name not in final_fields]

    if not new_cols:
        logging.info("✅ Schema sudah sinkron — tidak ada kolom baru")
        return "no_changes"

    for col in new_cols:
        alter_sql = (
            f"ALTER TABLE `{final_ref}` "
            f"ADD COLUMN IF NOT EXISTS `{col.name}` {col.field_type}"
        )
        logging.info("⚙️  Menambah kolom: %s (%s)", col.name, col.field_type)
        try:
            job = client.query(alter_sql)
            job.result(timeout=300)
        except Exception as exc:
            raise RuntimeError(
                f"Schema evolution gagal untuk kolom '{col.name}' (type: {col.field_type}). "
                f"Kemungkinan penyebab: type conflict atau insufficient permission. "
                f"Detail: {exc}"
            ) from exc

    added = [c.name for c in new_cols]
    logging.info("✅ Schema evolution selesai — kolom baru ditambahkan: %s", added)
    return f"added:{','.join(added)}"