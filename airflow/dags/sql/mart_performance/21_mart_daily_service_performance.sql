-- Grain:
-- metric_date x outlet_id x channel x service_id x unit_id x unit_name.
--
-- Primary uses:
-- - top service table
-- - top service ranking
-- - service revenue trend
-- - quantity trend after filtering one unit_name
--
-- Important:
-- total_quantity must not be compared or summed across different units.
-- Ranking services across different units should use revenue,
-- transaction_count, or service_line_count.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_daily_service_performance`
AS

WITH regular AS (
  SELECT
    l.transaction_date AS metric_date,
    l.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'REGULAR' AS channel,

    l.service_id,
    l.service_name,
    l.service_category_id,
    l.service_category_name,

    l.unit_id,
    COALESCE(
      ANY_VALUE(u.unit_name),
      ANY_VALUE(l.unit_name),
      'UNKNOWN'
    ) AS unit_name,

    COUNT(DISTINCT l.transaction_id)
      AS transaction_count,

    COUNT(*) AS service_line_count,

    COUNT(DISTINCT l.customer_uuid)
      AS customer_count,

    SUM(COALESCE(l.quantity, 0))
      AS total_quantity,

    SUM(COALESCE(l.line_amount, 0))
      AS direct_service_revenue,

    CAST(0 AS NUMERIC)
      AS estimated_deposit_value_consumed,

    CAST(0 AS NUMERIC)
      AS regular_price_equivalent

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_transaction_lines`
    AS l

  JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_transactions`
    AS h
    USING (transaction_id)

  LEFT JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_service_unit_reference`
    AS u
    USING (unit_id)

  WHERE h.cancelled_at_utc IS NULL

  GROUP BY
    metric_date,
    outlet_id,
    channel,
    service_id,
    service_name,
    service_category_id,
    service_category_name,
    unit_id
),

deposit_redemption AS (
  SELECT
    l.transaction_date AS metric_date,
    l.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'DEPOSIT_REDEMPTION' AS channel,

    l.regular_service_id AS service_id,
    l.regular_service_name AS service_name,
    l.service_category_id,
    l.service_category_name,

    l.unit_id,
    COALESCE(
      ANY_VALUE(u.unit_name),
      ANY_VALUE(l.unit_name),
      'UNKNOWN'
    ) AS unit_name,

    COUNT(DISTINCT l.package_transaction_id)
      AS transaction_count,

    COUNT(*) AS service_line_count,

    COUNT(DISTINCT l.customer_uuid)
      AS customer_count,

    SUM(COALESCE(l.redeemed_quantity, 0))
      AS total_quantity,

    CAST(0 AS NUMERIC)
      AS direct_service_revenue,

    SUM(COALESCE(
      l.estimated_deposit_value_consumed,
      0
    )) AS estimated_deposit_value_consumed,

    SUM(COALESCE(
      l.regular_price_equivalent,
      0
    )) AS regular_price_equivalent

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transaction_lines`
    AS l

  JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transactions`
    AS h
    USING (package_transaction_id)

  LEFT JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_service_unit_reference`
    AS u
    USING (unit_id)

  WHERE h.cancelled_at_utc IS NULL

  GROUP BY
    metric_date,
    outlet_id,
    channel,
    service_id,
    service_name,
    service_category_id,
    service_category_name,
    unit_id
),

self_service AS (
  SELECT
    l.transaction_date AS metric_date,
    h.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'SELF_SERVICE' AS channel,

    COALESCE(l.service_id, l.machine_id)
      AS service_id,

    COALESCE(
      l.service_name,
      l.machine_name,
      'Self Service'
    ) AS service_name,

    CAST(NULL AS STRING)
      AS service_category_id,

    'Self Service'
      AS service_category_name,

    CAST(14 AS INT64) AS unit_id,
    'LOAD' AS unit_name,

    COUNT(DISTINCT l.self_service_transaction_id)
      AS transaction_count,

    COUNT(*) AS service_line_count,

    COUNT(DISTINCT l.customer_uuid)
      AS customer_count,

    SUM(COALESCE(l.quantity, 1))
      AS total_quantity,

    SUM(COALESCE(l.line_amount, 0))
      AS direct_service_revenue,

    CAST(0 AS NUMERIC)
      AS estimated_deposit_value_consumed,

    CAST(0 AS NUMERIC)
      AS regular_price_equivalent

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_lines`
    AS l

  JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_transactions`
    AS h
    USING (self_service_transaction_id)

  GROUP BY
    metric_date,
    outlet_id,
    channel,
    service_id,
    service_name,
    service_category_id,
    service_category_name,
    unit_id,
    unit_name
)

SELECT * FROM regular
UNION ALL
SELECT * FROM deposit_redemption
UNION ALL
SELECT * FROM self_service;
