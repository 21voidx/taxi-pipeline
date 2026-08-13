CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchase_lines`
AS

WITH latest_parent AS (
  SELECT *
  FROM `{{ params.project_id }}.{{ params.raw_dataset }}.raw_deposit_purchase`
  WHERE id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(id AS STRING)
    ORDER BY
      SAFE_CAST(updated_at AS TIMESTAMP) DESC,
      SAFE_CAST(_ingested_at AS TIMESTAMP) DESC
  ) = 1
)

SELECT
  CAST(line.id AS STRING) AS deposit_purchase_line_id,
  CAST(t.id AS STRING) AS deposit_purchase_id,

  CAST(t.customer.uuid AS STRING) AS customer_uuid,

  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS purchase_date,

  CAST(
    line.company_outlet_regular_service_deposit_id AS STRING
  ) AS deposit_service_id,

  CAST(line.regular_service_deposit.name AS STRING)
    AS deposit_service_name,

  -- Unit is not included in this API payload.
  CAST(NULL AS STRING) AS unit_name,

  SAFE_CAST(line.quantity_purchase AS NUMERIC)
    AS package_count,

  SAFE_CAST(line.quantity_service AS NUMERIC)
    AS quantity_per_package,

  SAFE_CAST(line.quantity_total AS NUMERIC)
    AS quantity_purchased,

  SAFE_CAST(line.amount AS NUMERIC)
    AS package_unit_amount,

  SAFE_CAST(line.discount AS NUMERIC)
    AS package_discount_percent,

  SAFE_CAST(line.net_amount AS NUMERIC)
    AS package_net_amount,

  SAFE_CAST(line.total_amount AS NUMERIC)
    AS line_amount,

  SAFE_CAST(line.regular_service_deposit.price AS NUMERIC)
    AS deposit_service_price,

  SAFE_CAST(t.updated_at AS TIMESTAMP)
    AS source_updated_at_utc

FROM
  latest_parent AS t

CROSS JOIN UNNEST(t.deposit_services) AS line

WHERE line.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(line.id AS STRING)
  ORDER BY SAFE_CAST(t.updated_at AS TIMESTAMP) DESC
) = 1;
