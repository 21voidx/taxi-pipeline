CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_transaction_lines`
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
  CAST(line.id AS STRING) AS regular_transaction_line_id,
  CAST(t.id AS STRING) AS transaction_id,
  CAST(t.regular.id AS STRING) AS regular_detail_id,

  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.customer.uuid AS STRING) AS customer_uuid,
  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,

  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_date,

  CAST(line.service.id AS STRING) AS service_id,
  CAST(line.service.name AS STRING) AS service_name,

  CAST(line.service.category.id AS STRING) AS service_category_id,
  CAST(line.service.category.name AS STRING)
    AS service_category_name,

  SAFE_CAST(line.service.master_unit.id AS INT64) AS unit_id,
  UPPER(CAST(line.service.master_unit.name AS STRING)) AS unit_name,

  SAFE_CAST(line.quantity AS NUMERIC) AS quantity,
  SAFE_CAST(line.amount AS NUMERIC) AS unit_price,
  SAFE_CAST(line.discount AS NUMERIC) AS line_discount_amount,
  SAFE_CAST(line.net_amount AS NUMERIC) AS net_unit_price,
  SAFE_CAST(line.sub_total AS NUMERIC) AS line_amount,

  COALESCE(
    SAFE_CAST(line.service.is_can_scanning AS BOOL),
    SAFE_CAST(line.service.is_can_scanning AS INT64) != 0
  ) AS is_can_scanning,
  SAFE_CAST(line.service.multiply_scanning AS INT64)
    AS configured_multiply_scanning,

  SAFE_CAST(line.created_at AS TIMESTAMP) AS line_created_at_utc,
  SAFE_CAST(line.updated_at AS TIMESTAMP) AS line_updated_at_utc

FROM
  latest_parent AS t

CROSS JOIN UNNEST(
  COALESCE(t.regular.transaction_services, [])
) AS line

WHERE LOWER(CAST(t.transaction_type AS STRING)) = 'regular'

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(line.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(line.updated_at AS TIMESTAMP) DESC
) = 1;
