CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transaction_lines`
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
  CAST(line.id AS STRING) AS package_transaction_line_id,
  CAST(t.id AS STRING) AS package_transaction_id,
  CAST(t.package.id AS STRING) AS package_detail_id,

  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.customer.uuid AS STRING) AS customer_uuid,
  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,

  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_date,

  CAST(line.company_customer_deposit_package_id AS STRING)
    AS customer_deposit_package_id,

  CAST(line.company_outlet_regular_service_deposit_id AS STRING)
    AS deposit_service_id,
  CAST(line.service.name AS STRING) AS deposit_service_name,

  CAST(line.service.regular_service.id AS STRING)
    AS regular_service_id,
  CAST(line.service.regular_service.name AS STRING)
    AS regular_service_name,

  CAST(line.service.regular_service.category.id AS STRING)
    AS service_category_id,
  CAST(line.service.regular_service.category.name AS STRING)
    AS service_category_name,

  SAFE_CAST(
    line.service.regular_service.master_unit.id AS INT64
  ) AS unit_id,
  UPPER(CAST(
    line.service.regular_service.master_unit.name AS STRING
  )) AS unit_name,

  SAFE_CAST(line.quantity AS NUMERIC) AS redeemed_quantity,

  SAFE_CAST(line.service.quantity AS NUMERIC)
    AS package_total_quantity,
  SAFE_CAST(line.service.discount AS NUMERIC)
    AS package_discount_percent,
  SAFE_CAST(line.service.price AS NUMERIC)
    AS package_selling_price,
  SAFE_CAST(line.service.base_price AS NUMERIC)
    AS package_base_price,
  SAFE_CAST(line.service.expired_in_days AS INT64)
    AS package_expired_in_days,

  SAFE_CAST(line.service.regular_service.price AS NUMERIC)
    AS regular_unit_price,

  SAFE_CAST(line.amount AS NUMERIC) AS source_line_amount,

  SAFE_CAST(line.quantity AS NUMERIC)
    * SAFE_CAST(
        line.service.regular_service.price AS NUMERIC
      ) AS regular_price_equivalent,

  SAFE_DIVIDE(
    SAFE_CAST(line.service.price AS NUMERIC),
    NULLIF(SAFE_CAST(line.service.quantity AS NUMERIC), 0)
  ) AS effective_package_price_per_unit,

  SAFE_CAST(line.quantity AS NUMERIC)
    * SAFE_DIVIDE(
        SAFE_CAST(line.service.price AS NUMERIC),
        NULLIF(SAFE_CAST(line.service.quantity AS NUMERIC), 0)
      ) AS estimated_deposit_value_consumed,

  SAFE_CAST(
    line.service.regular_service.is_can_scanning AS BOOL
  ) AS is_can_scanning,

  SAFE_CAST(
    line.service.regular_service.multiply_scanning AS INT64
  ) AS configured_multiply_scanning,

  SAFE_CAST(line.created_at AS TIMESTAMP) AS line_created_at_utc,
  SAFE_CAST(line.updated_at AS TIMESTAMP) AS line_updated_at_utc

FROM
  latest_parent AS t

CROSS JOIN UNNEST(
  COALESCE(t.package.transaction_services, [])
) AS line

WHERE LOWER(CAST(t.transaction_type AS STRING)) = 'package'

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(line.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(line.updated_at AS TIMESTAMP) DESC
) = 1;
