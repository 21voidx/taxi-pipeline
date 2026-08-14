-- Grain: one row per metric_month x identified customer.
-- Only months in which the customer has at least one valid service order
-- appear in this mart.
--
-- Definitions:
-- New customer       = first identified service-order month is current month.
-- Returning customer = first month is earlier than current month.
-- Retained customer  = active in both current and immediately prior month.
-- Reactivated        = returning now, but not active in immediately prior month.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_monthly_customer_activity`
PARTITION BY metric_month
CLUSTER BY customer_uuid
AS

WITH monthly_base AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS metric_month,
    customer_uuid,
    ANY_VALUE(customer_name HAVING MAX order_created_at_utc)
      AS customer_name,

    COUNT(*) AS service_order_count,
    COUNTIF(channel = 'REGULAR') AS regular_order_count,
    COUNTIF(channel = 'SELF_SERVICE') AS self_service_order_count,
    COUNTIF(channel = 'DEPOSIT_REDEMPTION')
      AS deposit_redemption_count,

    SUM(direct_service_revenue)
      AS direct_service_revenue,
    SUM(estimated_deposit_value_consumed)
      AS estimated_deposit_value_consumed,
    SUM(total_service_value)
      AS total_service_value,
    SUM(cash_collected)
      AS cash_collected,

    SAFE_DIVIDE(
      SUM(total_service_value),
      NULLIF(COUNT(*), 0)
    ) AS average_order_value

  FROM
    `{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_order_events`

  WHERE customer_uuid IS NOT NULL

  GROUP BY
    metric_month,
    customer_uuid
),

sequenced AS (
  SELECT
    *,

    MIN(metric_month) OVER (
      PARTITION BY customer_uuid
    ) AS first_active_month,

    LAG(metric_month) OVER (
      PARTITION BY customer_uuid
      ORDER BY metric_month
    ) AS previous_active_month

  FROM monthly_base
)

SELECT
  *,

  metric_month = first_active_month
    AS is_new_customer,

  metric_month > first_active_month
    AS is_returning_customer,

  previous_active_month = DATE_SUB(
    metric_month,
    INTERVAL 1 MONTH
  ) AS was_active_previous_month,

  metric_month > first_active_month
    AND previous_active_month = DATE_SUB(
      metric_month,
      INTERVAL 1 MONTH
    ) AS is_retained_customer,

  metric_month > first_active_month
    AND (
      previous_active_month IS NULL
      OR previous_active_month != DATE_SUB(
        metric_month,
        INTERVAL 1 MONTH
      )
    ) AS is_reactivated_customer,

  DATE_DIFF(
    metric_month,
    previous_active_month,
    MONTH
  ) AS months_since_previous_active_month

FROM sequenced;