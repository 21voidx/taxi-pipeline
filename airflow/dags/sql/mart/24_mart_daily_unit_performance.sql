-- Grain:
-- metric_date x outlet_id x channel x unit_id x unit_name.
--
-- Primary uses:
-- - donut chart by unit_name
-- - unit usage trend
-- - distinct transaction count by unit
-- - quantity trend after selecting one unit
--
-- Donut recommendation:
-- dimension = unit_name
-- metric    = SUM(service_line_count)
--
-- Do not use SUM(total_quantity) for a donut across different units.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_daily_unit_performance`
AS

WITH service_events AS (
  -- Regular transaction service lines.
  SELECT
    l.transaction_date AS metric_date,
    l.outlet_id,
    h.outlet_name,
    'REGULAR' AS channel,

    l.transaction_id,
    l.customer_uuid,
    l.service_id,

    l.unit_id,
    COALESCE(
      u.unit_name,
      l.unit_name,
      'UNKNOWN'
    ) AS unit_name,

    COALESCE(l.quantity, 0)
      AS quantity,

    COALESCE(l.line_amount, 0)
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

  UNION ALL

  -- Deposit redemption service lines.
  SELECT
    l.transaction_date AS metric_date,
    l.outlet_id,
    h.outlet_name,
    'DEPOSIT_REDEMPTION' AS channel,

    l.package_transaction_id AS transaction_id,
    l.customer_uuid,
    l.regular_service_id AS service_id,

    l.unit_id,
    COALESCE(
      u.unit_name,
      l.unit_name,
      'UNKNOWN'
    ) AS unit_name,

    COALESCE(l.redeemed_quantity, 0)
      AS quantity,

    CAST(0 AS NUMERIC)
      AS direct_service_revenue,

    COALESCE(
      l.estimated_deposit_value_consumed,
      0
    ) AS estimated_deposit_value_consumed,

    COALESCE(
      l.regular_price_equivalent,
      0
    ) AS regular_price_equivalent

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

  UNION ALL

  -- Self-service: one washer/dryer use is one LOAD.
  SELECT
    l.transaction_date AS metric_date,
    h.outlet_id,
    h.outlet_name,
    'SELF_SERVICE' AS channel,

    l.self_service_transaction_id AS transaction_id,
    l.customer_uuid,
    COALESCE(l.service_id, l.machine_id) AS service_id,

    CAST(14 AS INT64) AS unit_id,
    'LOAD' AS unit_name,

    COALESCE(l.quantity, 1)
      AS quantity,

    COALESCE(l.line_amount, 0)
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
)

SELECT
  metric_date,
  outlet_id,
  ANY_VALUE(outlet_name) AS outlet_name,
  channel,

  unit_id,
  unit_name,

  COUNT(DISTINCT transaction_id)
    AS transaction_count,

  COUNT(*) AS service_line_count,

  COUNT(DISTINCT service_id)
    AS distinct_service_count,

  COUNT(DISTINCT customer_uuid)
    AS customer_count,

  SUM(quantity)
    AS total_quantity,

  SUM(direct_service_revenue)
    AS direct_service_revenue,

  SUM(estimated_deposit_value_consumed)
    AS estimated_deposit_value_consumed,

  SUM(regular_price_equivalent)
    AS regular_price_equivalent

FROM service_events

GROUP BY
  metric_date,
  outlet_id,
  channel,
  unit_id,
  unit_name;
