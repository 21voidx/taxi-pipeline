CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_transaction_service_operations`
AS

WITH latest_parent AS (
  SELECT *
  FROM `{{ params.project_id }}.{{ params.raw_dataset }}.raw_transaction_detail`
  WHERE id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(id AS STRING)
    ORDER BY
      SAFE_CAST(updated_at AS TIMESTAMP) DESC,
      SAFE_CAST(_ingested_at AS TIMESTAMP) DESC
  ) = 1
)

SELECT
  CAST(op.id AS STRING) AS service_operation_id,
  CAST(t.id AS STRING) AS transaction_id,
  CAST(op.transaction_service_id AS STRING) AS transaction_service_id,

  LOWER(CAST(op.transaction_type AS STRING)) AS transaction_type,

  CAST(op.company_outlet_regular_service_id AS STRING)
    AS regular_service_id,
  CAST(op.company_outlet_regular_service_deposit_id AS STRING)
    AS deposit_service_id,

  CAST(op.service.name AS STRING) AS service_name,

  SAFE_CAST(op.master_unit_regular_service_id AS INT64) AS unit_id,
  UPPER(CAST(
    COALESCE(
      op.service.regular_service.master_unit.name,
      op.service.master_unit.name
    ) AS STRING
  )) AS unit_name,

  SAFE_CAST(
    COALESCE(
      op.service.regular_service.multiply_scanning,
      op.service.multiply_scanning
    ) AS INT64
  ) AS configured_multiply_scanning,

  SAFE_CAST(
    COALESCE(
      op.service.regular_service.is_can_scanning,
      op.service.is_can_scanning
    ) AS BOOL
  ) AS is_can_scanning,

  CAST(op.rack.id AS STRING) AS rack_id,
  CAST(op.rack.combined_name AS STRING) AS rack_name,
  SAFE_CAST(op.rack.number_rack AS INT64) AS rack_number,

  TIMESTAMP(
    SAFE_CAST(op.estimation_finish_at AS DATETIME),
    'Asia/Jakarta'
  ) AS estimation_finish_at_utc,

  SAFE_CAST(op.created_at AS TIMESTAMP) AS operation_created_at_utc,
  SAFE_CAST(op.updated_at AS TIMESTAMP) AS operation_updated_at_utc,

  SAFE_CAST(t.cancelled_at AS TIMESTAMP)
    AS transaction_cancelled_at_utc

FROM
  latest_parent AS t

CROSS JOIN UNNEST(
  COALESCE(t.relation_services, [])
) AS op

WHERE op.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(op.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(op.updated_at AS TIMESTAMP) DESC
) = 1;
