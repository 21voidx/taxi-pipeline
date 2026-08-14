-- Grain: one row per identified customer.
--
-- RFM definition in v8:
-- R = days since last valid service order as of CURRENT_DATE Asia/Jakarta
-- F = number of valid identified service orders
-- M = total service value, including deposit value consumed
--
-- Scores use explicit business thresholds so they remain understandable and
-- stable. Review thresholds periodically as transaction volume grows.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_analytics`
CLUSTER BY rfm_segment, customer_lifecycle_segment, channel_segment
AS

WITH rfm_config AS (
  SELECT
    CURRENT_DATE('Asia/Jakarta') AS analysis_date,

    30 AS recency_score_5_max_days,
    60 AS recency_score_4_max_days,
    90 AS recency_score_3_max_days,
    180 AS recency_score_2_max_days,

    12 AS frequency_score_5_min_orders,
    8 AS frequency_score_4_min_orders,
    4 AS frequency_score_3_min_orders,
    2 AS frequency_score_2_min_orders,

    CAST(1000000 AS NUMERIC)
      AS monetary_score_5_min_value,
    CAST(500000 AS NUMERIC)
      AS monetary_score_4_min_value,
    CAST(250000 AS NUMERIC)
      AS monetary_score_3_min_value,
    CAST(100000 AS NUMERIC)
      AS monetary_score_2_min_value
),

customer_keys AS (
  SELECT customer_uuid
  FROM `{{ params.project_id }}.{{ params.stg_dataset }}.stg_customer`
  WHERE customer_uuid IS NOT NULL

  UNION DISTINCT

  SELECT customer_uuid
  FROM `{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_order_events`
  WHERE customer_uuid IS NOT NULL

  UNION DISTINCT

  SELECT customer_uuid
  FROM `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchases`
  WHERE customer_uuid IS NOT NULL
),

order_summary AS (
  SELECT
    customer_uuid,

    COUNT(*) AS identified_service_order_count,
    COUNTIF(channel = 'REGULAR')
      AS regular_transaction_count,
    COUNTIF(channel = 'SELF_SERVICE')
      AS self_service_transaction_count,
    COUNTIF(channel = 'DEPOSIT_REDEMPTION')
      AS deposit_redemption_count,

    SUM(direct_service_revenue)
      AS direct_service_spend,
    SUM(estimated_deposit_value_consumed)
      AS estimated_deposit_value_consumed,
    SUM(total_service_value)
      AS total_service_value,
    SUM(cash_collected)
      AS service_order_cash_collected,
    SUM(outstanding_amount)
      AS regular_outstanding_amount,

    MIN(order_date) AS first_service_order_date,
    MAX(order_date) AS last_service_order_date,

    MIN(IF(channel = 'REGULAR', order_date, NULL))
      AS first_regular_date,
    MAX(IF(channel = 'REGULAR', order_date, NULL))
      AS last_regular_date,

    MIN(IF(channel = 'SELF_SERVICE', order_date, NULL))
      AS first_self_service_date,
    MAX(IF(channel = 'SELF_SERVICE', order_date, NULL))
      AS last_self_service_date,

    MIN(IF(channel = 'DEPOSIT_REDEMPTION', order_date, NULL))
      AS first_deposit_redemption_date,
    MAX(IF(channel = 'DEPOSIT_REDEMPTION', order_date, NULL))
      AS last_deposit_redemption_date

  FROM
    `{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_order_events`

  GROUP BY customer_uuid
),

deposit_purchase_header_summary AS (
  SELECT
    customer_uuid,

    COUNTIF(cancelled_at_utc IS NULL)
      AS deposit_purchase_count,

    SUM(IF(
      cancelled_at_utc IS NULL,
      COALESCE(deposit_cash_in, 0),
      0
    )) AS deposit_cash_in,

    MIN(IF(cancelled_at_utc IS NULL, purchase_date, NULL))
      AS first_deposit_purchase_date,

    MAX(IF(cancelled_at_utc IS NULL, purchase_date, NULL))
      AS last_deposit_purchase_date

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchases`

  WHERE customer_uuid IS NOT NULL
  GROUP BY customer_uuid
),

deposit_purchase_line_summary AS (
  SELECT
    l.customer_uuid,

    SUM(COALESCE(l.quantity_purchased, 0))
      AS source_deposit_quantity_purchased,

    COUNT(*) AS deposit_purchase_line_count

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchase_lines`
    AS l

  JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchases`
    AS h
    USING (deposit_purchase_id)

  WHERE l.customer_uuid IS NOT NULL
    AND h.cancelled_at_utc IS NULL

  GROUP BY l.customer_uuid
),

deposit_redemption_quantity_summary AS (
  SELECT
    l.customer_uuid,

    SUM(COALESCE(l.redeemed_quantity, 0))
      AS source_deposit_quantity_redeemed,

    SUM(COALESCE(l.regular_price_equivalent, 0))
      AS deposit_regular_price_equivalent

  FROM
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transaction_lines`
    AS l

  JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transactions`
    AS h
    USING (package_transaction_id)

  WHERE l.customer_uuid IS NOT NULL
    AND h.cancelled_at_utc IS NULL

  GROUP BY l.customer_uuid
),

base AS (
  SELECT
    cfg.analysis_date,
    k.customer_uuid,
    c.customer_name,
    c.customer_code,
    c.honorific,
    c.is_membership,
    c.is_membership_deposit,
    c.is_active,
    c.customer_created_datetime_jakarta,

    COALESCE(o.identified_service_order_count, 0)
      AS identified_service_order_count,
    COALESCE(o.regular_transaction_count, 0)
      AS regular_transaction_count,
    COALESCE(o.self_service_transaction_count, 0)
      AS self_service_transaction_count,
    COALESCE(o.deposit_redemption_count, 0)
      AS deposit_redemption_count,

    COALESCE(o.direct_service_spend, 0)
      AS direct_service_spend,
    COALESCE(o.estimated_deposit_value_consumed, 0)
      AS estimated_deposit_value_consumed,
    COALESCE(o.total_service_value, 0)
      AS total_service_value,
    COALESCE(o.service_order_cash_collected, 0)
      AS service_order_cash_collected,
    COALESCE(o.regular_outstanding_amount, 0)
      AS regular_outstanding_amount,

    COALESCE(dp.deposit_purchase_count, 0)
      AS deposit_purchase_count,
    COALESCE(dp.deposit_cash_in, 0)
      AS deposit_cash_in,

    COALESCE(dpl.source_deposit_quantity_purchased, 0)
      AS source_deposit_quantity_purchased,
    COALESCE(dpl.deposit_purchase_line_count, 0)
      AS deposit_purchase_line_count,
    COALESCE(drq.source_deposit_quantity_redeemed, 0)
      AS source_deposit_quantity_redeemed,
    COALESCE(drq.deposit_regular_price_equivalent, 0)
      AS deposit_regular_price_equivalent,

    o.first_service_order_date,
    o.last_service_order_date,
    o.first_regular_date,
    o.last_regular_date,
    o.first_self_service_date,
    o.last_self_service_date,
    dp.first_deposit_purchase_date,
    dp.last_deposit_purchase_date,
    o.first_deposit_redemption_date,
    o.last_deposit_redemption_date,

    cfg.recency_score_5_max_days,
    cfg.recency_score_4_max_days,
    cfg.recency_score_3_max_days,
    cfg.recency_score_2_max_days,
    cfg.frequency_score_5_min_orders,
    cfg.frequency_score_4_min_orders,
    cfg.frequency_score_3_min_orders,
    cfg.frequency_score_2_min_orders,
    cfg.monetary_score_5_min_value,
    cfg.monetary_score_4_min_value,
    cfg.monetary_score_3_min_value,
    cfg.monetary_score_2_min_value

  FROM customer_keys AS k
  CROSS JOIN rfm_config AS cfg

  LEFT JOIN
    `{{ params.project_id }}.{{ params.stg_dataset }}.stg_customer`
    AS c
    USING (customer_uuid)

  LEFT JOIN order_summary AS o
    USING (customer_uuid)

  LEFT JOIN deposit_purchase_header_summary AS dp
    USING (customer_uuid)

  LEFT JOIN deposit_purchase_line_summary AS dpl
    USING (customer_uuid)

  LEFT JOIN deposit_redemption_quantity_summary AS drq
    USING (customer_uuid)
),

metrics AS (
  SELECT
    *,

    DATE_DIFF(
      analysis_date,
      last_service_order_date,
      DAY
    ) AS recency_days,

    DATE_DIFF(
      analysis_date,
      first_service_order_date,
      DAY
    ) AS customer_tenure_days,

    identified_service_order_count >= 2
      AS is_repeat_customer,

    SAFE_DIVIDE(
      direct_service_spend,
      NULLIF(
        regular_transaction_count
          + self_service_transaction_count,
        0
      )
    ) AS average_direct_service_order_value,

    SAFE_DIVIDE(
      total_service_value,
      NULLIF(identified_service_order_count, 0)
    ) AS average_total_service_order_value,

    SAFE_DIVIDE(
      service_order_cash_collected,
      NULLIF(identified_service_order_count, 0)
    ) AS average_cash_collected_per_service_order,

    SAFE_DIVIDE(
      deposit_cash_in,
      NULLIF(deposit_purchase_count, 0)
    ) AS average_deposit_purchase_value,

    source_deposit_quantity_purchased
      - source_deposit_quantity_redeemed
      AS source_estimated_deposit_quantity_remaining,

    (
      SELECT MIN(activity_date)
      FROM UNNEST([
        first_service_order_date,
        first_deposit_purchase_date
      ]) AS activity_date
      WHERE activity_date IS NOT NULL
    ) AS first_commercial_activity_date,

    (
      SELECT MAX(activity_date)
      FROM UNNEST([
        last_service_order_date,
        last_deposit_purchase_date
      ]) AS activity_date
      WHERE activity_date IS NOT NULL
    ) AS last_commercial_activity_date

  FROM base
),

scored AS (
  SELECT
    *,

    CASE
      WHEN identified_service_order_count = 0 THEN 0
      WHEN recency_days <= recency_score_5_max_days THEN 5
      WHEN recency_days <= recency_score_4_max_days THEN 4
      WHEN recency_days <= recency_score_3_max_days THEN 3
      WHEN recency_days <= recency_score_2_max_days THEN 2
      ELSE 1
    END AS recency_score,

    CASE
      WHEN identified_service_order_count = 0 THEN 0
      WHEN identified_service_order_count >= frequency_score_5_min_orders
        THEN 5
      WHEN identified_service_order_count >= frequency_score_4_min_orders
        THEN 4
      WHEN identified_service_order_count >= frequency_score_3_min_orders
        THEN 3
      WHEN identified_service_order_count >= frequency_score_2_min_orders
        THEN 2
      ELSE 1
    END AS frequency_score,

    CASE
      WHEN identified_service_order_count = 0 THEN 0
      WHEN total_service_value >= monetary_score_5_min_value THEN 5
      WHEN total_service_value >= monetary_score_4_min_value THEN 4
      WHEN total_service_value >= monetary_score_3_min_value THEN 3
      WHEN total_service_value >= monetary_score_2_min_value THEN 2
      ELSE 1
    END AS monetary_score

  FROM metrics
)

SELECT
  * EXCEPT (
    recency_score_5_max_days,
    recency_score_4_max_days,
    recency_score_3_max_days,
    recency_score_2_max_days,
    frequency_score_5_min_orders,
    frequency_score_4_min_orders,
    frequency_score_3_min_orders,
    frequency_score_2_min_orders,
    monetary_score_5_min_value,
    monetary_score_4_min_value,
    monetary_score_3_min_value,
    monetary_score_2_min_value
  ),

  recency_score + frequency_score + monetary_score
    AS rfm_total_score,

  CONCAT(
    CAST(recency_score AS STRING),
    CAST(frequency_score AS STRING),
    CAST(monetary_score AS STRING)
  ) AS rfm_code,

  'BUSINESS_THRESHOLDS_V1' AS rfm_scoring_method,

  CASE
    WHEN identified_service_order_count = 0
      THEN 'NO_SERVICE_ORDER'

    WHEN customer_tenure_days <= 30
      AND identified_service_order_count = 1
      THEN 'NEW'

    WHEN recency_score = 5
      AND frequency_score >= 4
      AND monetary_score >= 4
      THEN 'CHAMPION'

    WHEN recency_score >= 4
      AND frequency_score >= 3
      THEN 'LOYAL'

    WHEN recency_score >= 4
      AND frequency_score = 2
      THEN 'POTENTIAL_LOYALIST'

    WHEN recency_score <= 2
      AND frequency_score >= 3
      THEN 'CHURN_RISK'

    WHEN recency_score = 1
      AND frequency_score <= 2
      THEN 'HIBERNATING'

    WHEN recency_score = 3
      AND frequency_score >= 2
      THEN 'NEEDS_ATTENTION'

    ELSE 'ACTIVE'
  END AS rfm_segment,

  CASE
    WHEN identified_service_order_count = 0
      THEN 'NO_SERVICE_ORDER'

    WHEN customer_tenure_days <= 30
      AND identified_service_order_count = 1
      THEN 'NEW'

    WHEN recency_days <= 60
      AND identified_service_order_count >= 4
      THEN 'LOYAL'

    WHEN recency_days > 60
      AND recency_days <= 180
      AND identified_service_order_count >= 2
      THEN 'CHURN_RISK'

    WHEN recency_days > 180
      THEN 'LAPSED'

    ELSE 'ACTIVE'
  END AS customer_lifecycle_segment,

  CASE
    WHEN regular_transaction_count > 0
      AND self_service_transaction_count = 0
      AND deposit_purchase_count = 0
      AND deposit_redemption_count = 0
      THEN 'REGULAR_ONLY'

    WHEN self_service_transaction_count > 0
      AND regular_transaction_count = 0
      AND deposit_purchase_count = 0
      AND deposit_redemption_count = 0
      THEN 'SELF_SERVICE_ONLY'

    WHEN deposit_purchase_count > 0
      AND deposit_redemption_count = 0
      AND regular_transaction_count = 0
      AND self_service_transaction_count = 0
      THEN 'DEPOSIT_PURCHASER_NOT_USED'

    WHEN deposit_redemption_count > 0
      AND regular_transaction_count = 0
      AND self_service_transaction_count = 0
      THEN 'ACTIVE_DEPOSIT_USER'

    WHEN identified_service_order_count = 0
      AND deposit_purchase_count = 0
      THEN 'NO_ACTIVITY'

    ELSE 'MULTI_CHANNEL'
  END AS channel_segment

FROM scored;