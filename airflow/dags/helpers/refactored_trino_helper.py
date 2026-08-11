"""
Reusable Airflow helper for time-based ingestion:
PostgreSQL -> BigQuery through Trino cross-catalog SQL.

Design principles
-----------------
* PostgreSQL timestamps in this project are TIMESTAMP WITHOUT TIME ZONE stored in UTC.
* Extraction watermark and BigQuery partition field are separate concepts.
* Mutable source tables use idempotent BigQuery MERGE (current-state upsert).
* Immutable event tables can use insert-only MERGE by setting append_only=True.
* A per-table lookback protects against late commits and delayed updates.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Literal


# =============================================================================
# Layer 1: pure utilities (no Airflow dependency)
# =============================================================================

BQ_TO_TRINO_CAST: dict[str, str] = {
    "INTEGER": "BIGINT",
    "INT64": "BIGINT",
    "FLOAT": "DOUBLE",
    "FLOAT64": "DOUBLE",
    "STRING": "VARCHAR",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "NUMERIC": "DECIMAL(38,9)",
    "BIGNUMERIC": "DECIMAL(38,9)",
    "DATE": "DATE",
}

TRINO_TIMESTAMP_TYPE = "TIMESTAMP(6) WITH TIME ZONE"

METADATA_SCHEMA_FIELDS: list[dict[str, str]] = [
    {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "_batch_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"},
    {"name": "_source_updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "_airflow_run_id", "type": "STRING", "mode": "REQUIRED"},
]
METADATA_NAMES = [field["name"] for field in METADATA_SCHEMA_FIELDS]

ExtractionMode = Literal["incremental", "full"]


def quote_sql_string(value: str) -> str:
    """Escape a value used inside a SQL string literal."""
    return value.replace("'", "''")


def build_schema_lookup(schema_fields: list[dict[str, Any]]) -> dict[str, str]:
    """Return {column_name: BQ type} from source schema fields."""
    return {field["name"]: field["type"].upper() for field in schema_fields}


def effective_schema_fields(schema_fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Append standard ingestion metadata to source schema fields."""
    source_names = {field["name"] for field in schema_fields}
    duplicated = source_names.intersection(METADATA_NAMES)
    if duplicated:
        raise ValueError(
            "Metadata columns must not be declared in schema_fields: "
            + ", ".join(sorted(duplicated))
        )
    return [*schema_fields, *METADATA_SCHEMA_FIELDS]


def parse_columns(table_columns_str: str, schema_lookup: dict[str, str]) -> list[str]:
    """Parse table_columns and validate each source column."""
    columns: list[str] = []

    for raw_col in table_columns_str.replace("\n", ",").split(","):
        col = raw_col.strip()
        if not col:
            continue
        if col in METADATA_NAMES:
            raise ValueError(f"Metadata column '{col}' is added automatically.")
        if col not in schema_lookup:
            raise ValueError(
                f"Column '{col}' is not found in schema_fields. "
                "Fix table_columns or schema_fields."
            )
        columns.append(col)

    if not columns:
        raise ValueError("table_columns is empty after parsing.")
    if len(columns) != len(set(columns)):
        raise ValueError("table_columns contains duplicate columns.")
    return columns


def normalize_key_list(merge_key: str | list[str]) -> list[str]:
    """Normalize a single or composite key into a non-empty list."""
    values = [merge_key] if isinstance(merge_key, str) else merge_key
    keys = [key.strip() for key in values if key and key.strip()]
    if not keys:
        raise ValueError("merge_key must contain at least one column.")
    return keys


def prefixed_columns(columns: list[str], prefix: str | None = None) -> str:
    """Return a comma-separated identifier list with an optional alias prefix."""
    if prefix:
        return ", ".join(f"{prefix}.{column}" for column in columns)
    return ", ".join(columns)


def build_merge_condition(merge_key: str | list[str]) -> str:
    """Build a null-safe-enough MERGE key condition for required primary keys."""
    return " AND ".join(
        f"T.{key} = S.{key}" for key in normalize_key_list(merge_key)
    )


def build_trino_columns(
    *,
    columns: list[str],
    schema_lookup: dict[str, str],
    json_columns: list[str] | None = None,
    source_timestamp_timezone: str = "UTC",
) -> list[str]:
    """
    Normalize PostgreSQL values for the Trino BigQuery connector.

    PostgreSQL source timestamps in this project are naive UTC timestamps.
    `AT TIME ZONE 'UTC'` attaches the correct timezone without shifting the
    business event by seven hours.
    """
    json_set = set(json_columns or [])
    expressions: list[str] = []

    for col in columns:
        bq_type = schema_lookup[col].upper()

        if col in json_set:
            expressions.append(f"json_format(src.{col}) AS {col}")
        elif bq_type == "TIMESTAMP":
            expressions.append(
                f"CAST(src.{col} AT TIME ZONE '{source_timestamp_timezone}' "
                f"AS {TRINO_TIMESTAMP_TYPE}) AS {col}"
            )
        elif bq_type == "DATETIME":
            expressions.append(f"CAST(src.{col} AS TIMESTAMP(6)) AS {col}")
        elif bq_type in BQ_TO_TRINO_CAST:
            expressions.append(
                f"CAST(src.{col} AS {BQ_TO_TRINO_CAST[bq_type]}) AS {col}"
            )
        else:
            expressions.append(f"src.{col}")

    return expressions


def build_metadata_exprs(
    *,
    source_system: str,
    watermark_field: str,
    source_timestamp_timezone: str,
    batch_id_template: str,
    airflow_run_id_template: str,
) -> list[str]:
    """Build standard ingestion metadata expressions."""
    safe_source = quote_sql_string(source_system)
    return [
        f"CAST(CURRENT_TIMESTAMP AS {TRINO_TIMESTAMP_TYPE}) AS _ingested_at",
        f"CAST('{batch_id_template}' AS VARCHAR) AS _batch_id",
        f"CAST('{safe_source}' AS VARCHAR) AS _source_system",
        (
            f"CAST(src.{watermark_field} AT TIME ZONE "
            f"'{source_timestamp_timezone}' AS {TRINO_TIMESTAMP_TYPE}) "
            "AS _source_updated_at"
        ),
        f"CAST('{airflow_run_id_template}' AS VARCHAR) AS _airflow_run_id",
    ]


def build_window_predicate(
    *,
    watermark_field: str,
    extraction_mode: ExtractionMode,
    lookback_days: int,
    window_start_template: str,
    window_end_template: str,
) -> str:
    """Build a half-open [start-lookback, end) source watermark predicate."""
    if extraction_mode == "full":
        return ""
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")

    return f"""
    WHERE src.{watermark_field} >= date_add(
            'day',
            -{lookback_days},
            TIMESTAMP '{window_start_template}'
        )
      AND src.{watermark_field} < TIMESTAMP '{window_end_template}'
""".rstrip()


def build_trino_insert_sql(
    *,
    trino_bq_catalog: str,
    trino_pg_catalog: str,
    bq_dataset: str,
    bq_temp_table: str,
    pg_schema: str,
    pg_source_table: str,
    merge_key: str | list[str],
    watermark_field: str,
    source_order_fields: list[str],
    extraction_mode: ExtractionMode,
    lookback_days: int,
    columns: list[str],
    trino_columns: list[str],
    metadata_exprs: list[str],
    window_start_template: str,
    window_end_template: str,
) -> str:
    """Build Trino INSERT SQL from PostgreSQL into a BigQuery temp table."""
    keys = normalize_key_list(merge_key)
    order_fields = source_order_fields or [watermark_field]

    all_names = columns + METADATA_NAMES
    all_exprs = trino_columns + metadata_exprs

    insert_cols = ",\n        ".join(all_names)
    select_exprs = ",\n            ".join(all_exprs)
    final_cols = ", ".join(all_names)
    partition_by = prefixed_columns(keys, prefix="src")
    order_by = ", ".join(f"src.{column} DESC" for column in order_fields)
    predicate = build_window_predicate(
        watermark_field=watermark_field,
        extraction_mode=extraction_mode,
        lookback_days=lookback_days,
        window_start_template=window_start_template,
        window_end_template=window_end_template,
    )

    return f"""
INSERT INTO {trino_bq_catalog}.{bq_dataset}.{bq_temp_table} (
    {insert_cols}
)
WITH ranked AS (
    SELECT
        {select_exprs},
        ROW_NUMBER() OVER (
            PARTITION BY {partition_by}
            ORDER BY {order_by}
        ) AS _rn
    FROM {trino_pg_catalog}.{pg_schema}.{pg_source_table} AS src
{predicate}
)
SELECT {final_cols}
FROM ranked
WHERE _rn = 1
""".strip()


def build_table_resource(
    *,
    bq_project: str,
    bq_dataset: str,
    table_id: str,
    schema_fields: list[dict[str, Any]],
    partition_field: str | None = None,
    partition_type: str = "DAY",
    cluster_fields: list[str] | None = None,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build BigQuery table_resource for final or temporary tables."""
    resource: dict[str, Any] = {
        "tableReference": {
            "projectId": bq_project,
            "datasetId": bq_dataset,
            "tableId": table_id,
        },
        "schema": {"fields": effective_schema_fields(schema_fields)},
    }

    if partition_field:
        resource["timePartitioning"] = {
            "type": partition_type,
            "field": partition_field,
        }
    if cluster_fields:
        resource["clustering"] = {"fields": cluster_fields}
    if labels:
        resource["labels"] = labels
    return resource


def build_update_condition(version_field: str | None = None) -> str:
    """Only replace a target row when the source version is newer."""
    conditions = [
        "T._source_updated_at IS NULL",
        "S._source_updated_at > T._source_updated_at",
    ]
    if version_field:
        conditions.append(
            "(S._source_updated_at = T._source_updated_at "
            f"AND COALESCE(S.{version_field}, 0) > COALESCE(T.{version_field}, 0))"
        )
    return " OR ".join(conditions)


def build_bq_merge_query(
    *,
    bq_project: str,
    bq_dataset: str,
    bq_final_table: str,
    bq_temp_table: str,
    merge_key: str | list[str],
    columns: list[str],
    append_only: bool = False,
    version_field: str | None = None,
    sync_deletes: bool = False,
    job_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build idempotent BigQuery MERGE for mutable or immutable source rows."""
    keys = normalize_key_list(merge_key)
    key_columns = set(keys)
    all_cols = columns + METADATA_NAMES

    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"S.{col}" for col in all_cols)
    source_order = ["_source_updated_at DESC"]
    if version_field:
        source_order.append(f"{version_field} DESC")
    source_order.append("_ingested_at DESC")

    clauses: list[str] = []
    if not append_only:
        update_cols = [col for col in all_cols if col not in key_columns]
        set_clause = ",\n                        ".join(
            f"{col} = S.{col}" for col in update_cols
        )
        clauses.append(
            f"""WHEN MATCHED AND ({build_update_condition(version_field)}) THEN
                    UPDATE SET
                        {set_clause}"""
        )

    clauses.append(
        f"""WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})"""
    )

    if sync_deletes:
        if append_only:
            raise ValueError("sync_deletes cannot be used with append_only=True")
        clauses.append("WHEN NOT MATCHED BY SOURCE THEN DELETE")

    merge_sql = f"""
MERGE `{bq_project}.{bq_dataset}.{bq_final_table}` AS T
USING (
    SELECT * EXCEPT(_rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {prefixed_columns(keys)}
                ORDER BY {', '.join(source_order)}
            ) AS _rn
        FROM `{bq_project}.{bq_dataset}.{bq_temp_table}`
    )
    WHERE _rn = 1
) AS S
ON {build_merge_condition(merge_key)}
{'\n'.join(clauses)}
""".strip()

    configuration: dict[str, Any] = {
        "query": {
            "query": merge_sql,
            "useLegacySql": False,
            "defaultDataset": {
                "projectId": bq_project,
                "datasetId": bq_dataset,
            },
        }
    }
    if job_labels:
        configuration["labels"] = job_labels
    return configuration


def normalize_label_part(value: str) -> str:
    """Normalize strings into BigQuery-safe labels."""
    normalized = value.lower().replace("_", "-")
    normalized = re.sub(r"[^a-z0-9_-]", "-", normalized)
    normalized = re.sub(r"-+", "-", normalized).strip("-_")
    return normalized[:63] or "unknown"


def build_effective_labels(
    *,
    dag_labels: dict[str, str] | None,
    table_labels: dict[str, str] | None,
    pg_table: str,
    source_system: str,
) -> dict[str, str]:
    """Merge DAG, generated, and table-level labels."""
    merged = {
        **(dag_labels or {}),
        "table": pg_table,
        "source-system": source_system,
        **(table_labels or {}),
    }
    return {
        normalize_label_part(str(key)): normalize_label_part(str(value))
        for key, value in merged.items()
    }


def sync_final_table_schema(
    *,
    gcp_conn_id: str,
    bq_project: str,
    bq_dataset: str,
    bq_final_table: str,
    bq_temp_table: str,
    **_: Any,
) -> str:
    """Add newly introduced nullable columns from temp to an existing final table."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client(project_id=bq_project)

    temp_ref = f"{bq_project}.{bq_dataset}.{bq_temp_table}"
    final_ref = f"{bq_project}.{bq_dataset}.{bq_final_table}"

    try:
        temp_table = client.get_table(temp_ref)
        final_table = client.get_table(final_ref)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read BigQuery schemas for '{temp_ref}' and '{final_ref}': {exc}"
        ) from exc

    final_columns = {field.name for field in final_table.schema}
    new_columns = [field for field in temp_table.schema if field.name not in final_columns]

    for column in new_columns:
        # BigQuery adds columns as NULLABLE to an existing non-empty table.
        sql = (
            f"ALTER TABLE `{final_ref}` "
            f"ADD COLUMN IF NOT EXISTS `{column.name}` {column.field_type}"
        )
        try:
            client.query(sql).result(timeout=300)
            logging.info("Added %s.%s (%s)", final_ref, column.name, column.field_type)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to add column '{column.name}' to '{final_ref}': {exc}"
            ) from exc

    if not new_columns:
        return "no_changes"
    return "added:" + ",".join(column.name for column in new_columns)


# =============================================================================
# Layer 2: Airflow TaskGroup factory
# =============================================================================

from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.sdk import Asset
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


@dataclass(frozen=True)
class TableConfig:
    pg_table: str
    bq_final_table: str
    merge_key: str | list[str]
    watermark_field: str
    schema_fields: list[dict[str, Any]]
    table_columns: str

    extraction_mode: ExtractionMode = "incremental"
    lookback_days: int = 2
    source_order_fields: list[str] = field(default_factory=list)
    version_field: str | None = None

    bq_partition_field: str | None = None
    bq_partition_type: str = "DAY"
    cluster_fields: list[str] = field(default_factory=list)

    append_only: bool = False
    sync_deletes: bool = False
    source_system: str = "postgresql"
    json_fields: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.extraction_mode not in {"incremental", "full"}:
            raise ValueError(f"Unsupported extraction_mode: {self.extraction_mode}")
        if self.sync_deletes and self.extraction_mode != "full":
            raise ValueError("sync_deletes=True requires extraction_mode='full'")
        if self.append_only and self.sync_deletes:
            raise ValueError("append_only and sync_deletes cannot both be True")


def make_table_task_group(
    cfg: TableConfig | dict[str, Any],
    *,
    bq_project: str,
    bq_dataset: str,
    bq_location: str,
    pg_schema: str,
    trino_conn_id: str,
    gcp_conn_id: str,
    trino_bq_cat: str,
    trino_pg_cat: str,
    source_timestamp_timezone: str = "UTC",
    window_task_id: str = "build_window",
    dag_labels: dict[str, str] | None = None,
) -> TaskGroup:
    """Create an idempotent PostgreSQL-to-BigQuery ingestion TaskGroup."""
    if isinstance(cfg, dict):
        cfg = TableConfig(**cfg)

    schema_lookup = build_schema_lookup(cfg.schema_fields)
    columns = parse_columns(cfg.table_columns, schema_lookup)

    known_columns = set(columns)
    for required in [*normalize_key_list(cfg.merge_key), cfg.watermark_field]:
        if required not in known_columns:
            raise ValueError(f"'{required}' is required but missing from {cfg.pg_table}")
    for field_name in cfg.source_order_fields:
        if field_name not in known_columns:
            raise ValueError(
                f"source_order_field '{field_name}' is missing from {cfg.pg_table}"
            )
    if cfg.version_field and cfg.version_field not in known_columns:
        raise ValueError(
            f"version_field '{cfg.version_field}' is missing from {cfg.pg_table}"
        )

    trino_columns = build_trino_columns(
        columns=columns,
        schema_lookup=schema_lookup,
        json_columns=cfg.json_fields,
        source_timestamp_timezone=source_timestamp_timezone,
    )

    window_start_template = (
        "{{ ti.xcom_pull(task_ids='" + window_task_id + "')['window_start_utc'] }}"
    )
    window_end_template = (
        "{{ ti.xcom_pull(task_ids='" + window_task_id + "')['window_end_utc'] }}"
    )
    batch_id_template = (
        "{{ ti.xcom_pull(task_ids='" + window_task_id + "')['batch_id'] }}"
    )
    airflow_run_id_template = "{{ run_id }}"

    metadata_exprs = build_metadata_exprs(
        source_system=cfg.source_system,
        watermark_field=cfg.watermark_field,
        source_timestamp_timezone=source_timestamp_timezone,
        batch_id_template=batch_id_template,
        airflow_run_id_template=airflow_run_id_template,
    )

    effective_labels = build_effective_labels(
        dag_labels=dag_labels,
        table_labels=cfg.labels,
        pg_table=cfg.pg_table,
        source_system=cfg.source_system,
    )

    # ts_nodash is unique for normal scheduled runs and valid in a BQ table ID.
    bq_temp_table = (f"{cfg.bq_final_table}_temp_{{{{ ts_nodash | lower }}}}")
    final_asset = Asset(f"bigquery://{bq_project}/{bq_dataset}/{cfg.bq_final_table}")

    with TaskGroup(group_id=f"load_{cfg.pg_table}") as task_group:
        ensure_final = BigQueryCreateTableOperator(
            task_id="ensure_bq_final_table",
            gcp_conn_id=gcp_conn_id,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_id=cfg.bq_final_table,
            table_resource=build_table_resource(
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                table_id=cfg.bq_final_table,
                schema_fields=cfg.schema_fields,
                partition_field=cfg.bq_partition_field,
                partition_type=cfg.bq_partition_type,
                cluster_fields=cfg.cluster_fields,
                labels=effective_labels,
            ),
            if_exists="ignore",
        )

        remove_stale_temp = BigQueryDeleteTableOperator(
            task_id="remove_stale_temp_table",
            gcp_conn_id=gcp_conn_id,
            deletion_dataset_table=f"{bq_project}.{bq_dataset}.{bq_temp_table}",
            ignore_if_missing=True,
        )

        create_temp = BigQueryCreateTableOperator(
            task_id="create_bq_temp_table",
            gcp_conn_id=gcp_conn_id,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_id=bq_temp_table,
            table_resource=build_table_resource(
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                table_id=bq_temp_table,
                schema_fields=cfg.schema_fields,
                labels={**effective_labels, "temporary": "true"},
            ),
            if_exists="ignore",
        )

        insert_to_temp = SQLExecuteQueryOperator(
            task_id="insert_source_to_bq_temp",
            conn_id=trino_conn_id,
            sql=build_trino_insert_sql(
                trino_bq_catalog=trino_bq_cat,
                trino_pg_catalog=trino_pg_cat,
                bq_dataset=bq_dataset,
                bq_temp_table=bq_temp_table,
                pg_schema=pg_schema,
                pg_source_table=cfg.pg_table,
                merge_key=cfg.merge_key,
                watermark_field=cfg.watermark_field,
                source_order_fields=cfg.source_order_fields,
                extraction_mode=cfg.extraction_mode,
                lookback_days=cfg.lookback_days,
                columns=columns,
                trino_columns=trino_columns,
                metadata_exprs=metadata_exprs,
                window_start_template=window_start_template,
                window_end_template=window_end_template,
            ),
            autocommit=True,
            do_xcom_push=False,
        )

        sync_schema = PythonOperator(
            task_id="sync_final_table_schema",
            python_callable=sync_final_table_schema,
            op_kwargs={
                "gcp_conn_id": gcp_conn_id,
                "bq_project": bq_project,
                "bq_dataset": bq_dataset,
                "bq_final_table": cfg.bq_final_table,
                "bq_temp_table": bq_temp_table,
            },
        )

        merge_to_final = BigQueryInsertJobOperator(
            task_id="merge_temp_to_final",
            gcp_conn_id=gcp_conn_id,
            location=bq_location,
            configuration=build_bq_merge_query(
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                bq_final_table=cfg.bq_final_table,
                bq_temp_table=bq_temp_table,
                merge_key=cfg.merge_key,
                columns=columns,
                append_only=cfg.append_only,
                version_field=cfg.version_field,
                sync_deletes=cfg.sync_deletes,
                job_labels=effective_labels,
            ),
            outlets=[final_asset],
        )

        drop_temp = BigQueryDeleteTableOperator(
            task_id="drop_bq_temp_table",
            gcp_conn_id=gcp_conn_id,
            deletion_dataset_table=f"{bq_project}.{bq_dataset}.{bq_temp_table}",
            ignore_if_missing=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        ensure_final >> remove_stale_temp >> create_temp >> insert_to_temp
        insert_to_temp >> sync_schema >> merge_to_final >> drop_temp

    return task_group
