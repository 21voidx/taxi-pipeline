-- Grain: metric_date x outlet_id x channel.
--
-- IMPORTANT:
-- This mart intentionally excludes quantity metrics.
--
-- Laundry services use different official units:
-- KG, M2, Barang, PCS, Lembar, Stel, M, Paket, Unit, Dudukan,
-- Pasang, CM2, Koin, Load, Helai, Mili, Biji, and CM.
--
-- Adding quantities across different units would create a meaningless total.
-- Quantity reporting belongs in mart_daily_service_performance, where unit_id
-- and unit_name are part of the grain.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_daily_business_performance`
AS

WITH regular AS (
  SELECT
    h.transaction_date AS metric_date,
    h.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'REGULAR' AS channel,

    COUNTIF(h.cancelled_at_utc IS NULL) AS transaction_count,
    COUNTIF(h.cancelled_at_utc IS NULL) AS service_order_count,

    COUNT(DISTINCT IF(
      h.cancelled_at_utc IS NULL,
      h.customer_uuid,
      NULL
    )) AS customer_count,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(h.amount_paid, 0),
      0
    )) AS cash_in_amount,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(h.net_amount_final, 0),
      0
    )) AS direct_service_revenue,

    CAST(0 AS NUMERIC) AS deposit_cash_in,
    CAST(0 AS NUMERIC) AS estimated_deposit_value_consumed,
    CAST(0 AS NUMERIC) AS regular_price_equivalent,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(h.amount_remaining, 0),
      0
    )) AS outstanding_amount,

    CAST(0 AS NUMERIC) AS payment_fee_amount,
    CAST(0 AS NUMERIC) AS merchant_net_amount,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND h.payment_status = 'paid'
    ) AS paid_transaction_count,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND COALESCE(h.payment_status, '') != 'paid'
    ) AS unpaid_transaction_count,

    COUNTIF(h.cancelled_at_utc IS NOT NULL)
      AS cancelled_transaction_count,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND h.is_late
    ) AS late_transaction_count,

    CAST(0 AS INT64) AS callback_failure_count

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_transactions` AS h
  GROUP BY metric_date, outlet_id
),

self_service_line_summary AS (
  SELECT
    self_service_transaction_id,
    COUNTIF(NOT is_callback_success) AS callback_failure_count
  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_lines`
  GROUP BY self_service_transaction_id
),

self_service AS (
  SELECT
    h.transaction_date AS metric_date,
    h.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'SELF_SERVICE' AS channel,

    COUNT(*) AS transaction_count,
    COUNT(*) AS service_order_count,
    COUNT(DISTINCT h.customer_uuid) AS customer_count,

    SUM(COALESCE(h.customer_charged_amount, 0))
      AS cash_in_amount,

    SUM(COALESCE(h.customer_charged_amount, 0))
      AS direct_service_revenue,

    CAST(0 AS NUMERIC) AS deposit_cash_in,
    CAST(0 AS NUMERIC) AS estimated_deposit_value_consumed,
    CAST(0 AS NUMERIC) AS regular_price_equivalent,
    CAST(0 AS NUMERIC) AS outstanding_amount,

    SUM(COALESCE(h.payment_fee_amount, 0))
      AS payment_fee_amount,

    SUM(COALESCE(
      h.merchant_net_amount,
      h.customer_charged_amount - COALESCE(h.payment_fee_amount, 0),
      0
    )) AS merchant_net_amount,

    COUNTIF(h.payment_status = 'paid')
      AS paid_transaction_count,

    COUNTIF(COALESCE(h.payment_status, '') != 'paid')
      AS unpaid_transaction_count,

    CAST(0 AS INT64) AS cancelled_transaction_count,
    CAST(0 AS INT64) AS late_transaction_count,

    SUM(COALESCE(l.callback_failure_count, 0))
      AS callback_failure_count

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_transactions` AS h
  LEFT JOIN self_service_line_summary AS l
    USING (self_service_transaction_id)
  GROUP BY metric_date, outlet_id
),

deposit_purchase AS (
  SELECT
    h.purchase_date AS metric_date,
    h.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'DEPOSIT_PURCHASE' AS channel,

    COUNTIF(h.cancelled_at_utc IS NULL) AS transaction_count,
    CAST(0 AS INT64) AS service_order_count,

    COUNT(DISTINCT IF(
      h.cancelled_at_utc IS NULL,
      h.customer_uuid,
      NULL
    )) AS customer_count,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(h.deposit_cash_in, 0),
      0
    )) AS cash_in_amount,

    CAST(0 AS NUMERIC) AS direct_service_revenue,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(h.deposit_cash_in, 0),
      0
    )) AS deposit_cash_in,

    CAST(0 AS NUMERIC) AS estimated_deposit_value_consumed,
    CAST(0 AS NUMERIC) AS regular_price_equivalent,
    CAST(0 AS NUMERIC) AS outstanding_amount,
    CAST(0 AS NUMERIC) AS payment_fee_amount,
    CAST(0 AS NUMERIC) AS merchant_net_amount,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND h.purchase_status IN (
        'paid', 'approved', 'success', 'completed'
      )
    ) AS paid_transaction_count,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND COALESCE(h.purchase_status, '')
        NOT IN ('paid', 'approved', 'success', 'completed')
    ) AS unpaid_transaction_count,

    COUNTIF(h.cancelled_at_utc IS NOT NULL)
      AS cancelled_transaction_count,

    CAST(0 AS INT64) AS late_transaction_count,
    CAST(0 AS INT64) AS callback_failure_count

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchases` AS h
  GROUP BY metric_date, outlet_id
),

package_line_summary AS (
  SELECT
    package_transaction_id,

    SUM(COALESCE(estimated_deposit_value_consumed, 0))
      AS estimated_deposit_value_consumed,

    SUM(COALESCE(regular_price_equivalent, 0))
      AS regular_price_equivalent

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transaction_lines`
  GROUP BY package_transaction_id
),

deposit_redemption AS (
  SELECT
    h.transaction_date AS metric_date,
    h.outlet_id,
    ANY_VALUE(h.outlet_name) AS outlet_name,
    'DEPOSIT_REDEMPTION' AS channel,

    COUNTIF(h.cancelled_at_utc IS NULL) AS transaction_count,
    COUNTIF(h.cancelled_at_utc IS NULL) AS service_order_count,

    COUNT(DISTINCT IF(
      h.cancelled_at_utc IS NULL,
      h.customer_uuid,
      NULL
    )) AS customer_count,

    CAST(0 AS NUMERIC) AS cash_in_amount,
    CAST(0 AS NUMERIC) AS direct_service_revenue,
    CAST(0 AS NUMERIC) AS deposit_cash_in,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(l.estimated_deposit_value_consumed, 0),
      0
    )) AS estimated_deposit_value_consumed,

    SUM(IF(
      h.cancelled_at_utc IS NULL,
      COALESCE(l.regular_price_equivalent, 0),
      0
    )) AS regular_price_equivalent,

    CAST(0 AS NUMERIC) AS outstanding_amount,
    CAST(0 AS NUMERIC) AS payment_fee_amount,
    CAST(0 AS NUMERIC) AS merchant_net_amount,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND h.payment_status = 'paid'
    ) AS paid_transaction_count,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND COALESCE(h.payment_status, '') != 'paid'
    ) AS unpaid_transaction_count,

    COUNTIF(h.cancelled_at_utc IS NOT NULL)
      AS cancelled_transaction_count,

    COUNTIF(
      h.cancelled_at_utc IS NULL
      AND h.is_late
    ) AS late_transaction_count,

    CAST(0 AS INT64) AS callback_failure_count

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transactions` AS h
  LEFT JOIN package_line_summary AS l
    USING (package_transaction_id)
  GROUP BY metric_date, outlet_id
)

SELECT * FROM regular
UNION ALL
SELECT * FROM self_service
UNION ALL
SELECT * FROM deposit_purchase
UNION ALL
SELECT * FROM deposit_redemption;
