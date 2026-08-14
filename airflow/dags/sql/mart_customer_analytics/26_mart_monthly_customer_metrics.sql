-- Grain: one row per calendar month.
-- Dashboard-ready acquisition, retention, churn and AOV metrics.
-- A month spine is generated so inactive months between first and last
-- observed service order remain visible with zero activity.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_monthly_customer_metrics`
PARTITION BY metric_month
AS

WITH bounds AS (
  SELECT
    MIN(metric_month) AS min_month,
    MAX(metric_month) AS max_month
  FROM
    `{{ params.project_id }}.{{ params.mart_dataset }}.mart_monthly_customer_activity`
),

month_spine AS (
  SELECT metric_month
  FROM bounds,
  UNNEST(
    IF(
      min_month IS NULL,
      ARRAY<DATE>[],
      GENERATE_DATE_ARRAY(
        min_month,
        max_month,
        INTERVAL 1 MONTH
      )
    )
  ) AS metric_month
),

monthly_summary AS (
  SELECT
    metric_month,

    COUNT(DISTINCT customer_uuid)
      AS active_customer_count,

    COUNT(DISTINCT IF(
      is_new_customer,
      customer_uuid,
      NULL
    )) AS new_customer_count,

    COUNT(DISTINCT IF(
      is_returning_customer,
      customer_uuid,
      NULL
    )) AS returning_customer_count,

    COUNT(DISTINCT IF(
      is_retained_customer,
      customer_uuid,
      NULL
    )) AS retained_customer_count,

    COUNT(DISTINCT IF(
      is_reactivated_customer,
      customer_uuid,
      NULL
    )) AS reactivated_customer_count,

    SUM(service_order_count)
      AS service_order_count,
    SUM(regular_order_count)
      AS regular_order_count,
    SUM(self_service_order_count)
      AS self_service_order_count,
    SUM(deposit_redemption_count)
      AS deposit_redemption_count,

    SUM(direct_service_revenue)
      AS direct_service_revenue,
    SUM(estimated_deposit_value_consumed)
      AS estimated_deposit_value_consumed,
    SUM(total_service_value)
      AS total_service_value,
    SUM(cash_collected)
      AS cash_collected

  FROM
    `{{ params.project_id }}.{{ params.mart_dataset }}.mart_monthly_customer_activity`

  GROUP BY metric_month
),

spined AS (
  SELECT
    m.metric_month,

    COALESCE(s.active_customer_count, 0)
      AS active_customer_count,
    COALESCE(s.new_customer_count, 0)
      AS new_customer_count,
    COALESCE(s.returning_customer_count, 0)
      AS returning_customer_count,
    COALESCE(s.retained_customer_count, 0)
      AS retained_customer_count,
    COALESCE(s.reactivated_customer_count, 0)
      AS reactivated_customer_count,

    COALESCE(s.service_order_count, 0)
      AS service_order_count,
    COALESCE(s.regular_order_count, 0)
      AS regular_order_count,
    COALESCE(s.self_service_order_count, 0)
      AS self_service_order_count,
    COALESCE(s.deposit_redemption_count, 0)
      AS deposit_redemption_count,

    COALESCE(s.direct_service_revenue, 0)
      AS direct_service_revenue,
    COALESCE(s.estimated_deposit_value_consumed, 0)
      AS estimated_deposit_value_consumed,
    COALESCE(s.total_service_value, 0)
      AS total_service_value,
    COALESCE(s.cash_collected, 0)
      AS cash_collected

  FROM month_spine AS m
  LEFT JOIN monthly_summary AS s
    USING (metric_month)
),

with_previous AS (
  SELECT
    *,

    LAG(active_customer_count) OVER (
      ORDER BY metric_month
    ) AS previous_month_active_customer_count

  FROM spined
)

SELECT
  *,

  GREATEST(
    COALESCE(previous_month_active_customer_count, 0)
      - retained_customer_count,
    0
  ) AS churned_customer_count,

  SAFE_DIVIDE(
    new_customer_count,
    NULLIF(active_customer_count, 0)
  ) AS new_customer_rate,

  SAFE_DIVIDE(
    returning_customer_count,
    NULLIF(active_customer_count, 0)
  ) AS returning_customer_rate,

  SAFE_DIVIDE(
    retained_customer_count,
    NULLIF(previous_month_active_customer_count, 0)
  ) AS monthly_retention_rate,

  SAFE_DIVIDE(
    GREATEST(
      COALESCE(previous_month_active_customer_count, 0)
        - retained_customer_count,
      0
    ),
    NULLIF(previous_month_active_customer_count, 0)
  ) AS monthly_churn_rate,

  SAFE_DIVIDE(
    total_service_value,
    NULLIF(service_order_count, 0)
  ) AS average_order_value,

  SAFE_DIVIDE(
    direct_service_revenue,
    NULLIF(
      regular_order_count + self_service_order_count,
      0
    )
  ) AS average_direct_order_value

FROM with_previous;