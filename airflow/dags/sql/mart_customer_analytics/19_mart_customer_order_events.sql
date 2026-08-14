-- Grain: one row per identified, valid customer service order.
--
-- This mart is the reusable customer-behaviour foundation for:
-- - RFM frequency and monetary value
-- - first/last service order
-- - monthly new vs returning customers
-- - retention and reactivation
-- - Average Order Value across service channels
--
-- Deposit purchase is intentionally excluded because it is a cash/package
-- purchase rather than a completed service order. Deposit redemption is
-- included as service value, but not as new cash-in.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_order_events`
PARTITION BY order_date
CLUSTER BY customer_uuid, channel
AS

WITH package_line_summary AS (
  SELECT
    l.package_transaction_id,

    SUM(COALESCE(l.estimated_deposit_value_consumed, 0))
      AS estimated_deposit_value_consumed,

    SUM(COALESCE(l.regular_price_equivalent, 0))
      AS regular_price_equivalent

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transaction_lines`
    AS l

  JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transactions`
    AS h
    USING (package_transaction_id)

  WHERE h.cancelled_at_utc IS NULL
  GROUP BY l.package_transaction_id
),

regular_orders AS (
  SELECT
    h.transaction_id AS order_id,
    h.note_number,
    h.transaction_date AS order_date,
    h.transaction_created_at_utc AS order_created_at_utc,

    h.customer_uuid,
    h.customer_name,
    h.outlet_id,
    h.outlet_name,

    'REGULAR' AS channel,
    h.payment_status,
    h.transaction_status,
    h.is_express,

    COALESCE(h.net_amount_final, 0)
      AS direct_service_revenue,

    CAST(0 AS NUMERIC)
      AS estimated_deposit_value_consumed,

    COALESCE(h.net_amount_final, 0)
      AS total_service_value,

    COALESCE(h.amount_paid, 0)
      AS cash_collected,

    COALESCE(h.amount_remaining, 0)
      AS outstanding_amount,

    h.payment_status IN (
      'paid', 'success', 'successful', 'completed'
    ) AS is_paid

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_transactions`
    AS h

  WHERE h.cancelled_at_utc IS NULL
    AND h.customer_uuid IS NOT NULL
),

self_service_orders AS (
  SELECT
    h.self_service_transaction_id AS order_id,
    h.note_number,
    h.transaction_date AS order_date,
    h.transaction_created_at_utc AS order_created_at_utc,

    h.customer_uuid,
    h.customer_name,
    h.outlet_id,
    h.outlet_name,

    'SELF_SERVICE' AS channel,
    h.payment_status,
    CAST(NULL AS STRING) AS transaction_status,
    FALSE AS is_express,

    COALESCE(h.customer_charged_amount, 0)
      AS direct_service_revenue,

    CAST(0 AS NUMERIC)
      AS estimated_deposit_value_consumed,

    COALESCE(h.customer_charged_amount, 0)
      AS total_service_value,

    COALESCE(h.customer_charged_amount, 0)
      AS cash_collected,

    CAST(0 AS NUMERIC)
      AS outstanding_amount,

    h.payment_status IN (
      'paid', 'success', 'successful', 'completed'
    ) AS is_paid

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_transactions`
    AS h

  WHERE h.customer_uuid IS NOT NULL
    AND h.payment_status IN (
      'paid', 'success', 'successful', 'completed'
    )
),

deposit_redemption_orders AS (
  SELECT
    h.package_transaction_id AS order_id,
    h.note_number,
    h.transaction_date AS order_date,
    h.transaction_created_at_utc AS order_created_at_utc,

    h.customer_uuid,
    h.customer_name,
    h.outlet_id,
    h.outlet_name,

    'DEPOSIT_REDEMPTION' AS channel,
    h.payment_status,
    h.transaction_status,
    FALSE AS is_express,

    CAST(0 AS NUMERIC)
      AS direct_service_revenue,

    COALESCE(p.estimated_deposit_value_consumed, 0)
      AS estimated_deposit_value_consumed,

    COALESCE(p.estimated_deposit_value_consumed, 0)
      AS total_service_value,

    CAST(0 AS NUMERIC)
      AS cash_collected,

    CAST(0 AS NUMERIC)
      AS outstanding_amount,

    h.payment_status IN (
      'paid', 'success', 'successful', 'completed'
    ) AS is_paid

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transactions`
    AS h

  LEFT JOIN package_line_summary AS p
    USING (package_transaction_id)

  WHERE h.cancelled_at_utc IS NULL
    AND h.customer_uuid IS NOT NULL
)

SELECT * FROM regular_orders
UNION ALL
SELECT * FROM self_service_orders
UNION ALL
SELECT * FROM deposit_redemption_orders;