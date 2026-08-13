-- Grain: one row per regular transaction.
--
-- Source of truth:
-- raw_transaction_detail
--
-- Enrichment only:
-- stg_transaction_index
--
-- raw_transaction_detail does NOT contain:
-- - t.cashier
-- - t.is_express

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_transactions`
AS

SELECT
  CAST(t.id AS STRING) AS transaction_id,
  CAST(t.regular.id AS STRING) AS regular_detail_id,

  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.ref_id AS STRING) AS ref_id,

  SAFE_CAST(t.company_id AS INT64) AS company_id,
  SAFE_CAST(t.company_customer_id AS INT64) AS customer_source_id,

  CAST(t.customer.uuid AS STRING) AS customer_uuid,
  CAST(t.customer.name AS STRING) AS customer_name,

  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,
  CAST(t.outlet.name AS STRING) AS outlet_name,

  SAFE_CAST(t.user_employee_id AS INT64) AS employee_id,
  idx.employee_name,

  LOWER(CAST(t.transaction_type AS STRING)) AS transaction_type,
  LOWER(CAST(t.payment_status AS STRING)) AS payment_status,
  LOWER(CAST(t.transaction_status AS STRING)) AS transaction_status,
  CAST(t.transaction_status_text AS STRING) AS transaction_status_text,
  CAST(t.position AS STRING) AS position_name,

  COALESCE(
    SAFE_CAST(t.is_late AS BOOL),
    SAFE_CAST(t.is_late AS INT64) != 0
  ) AS is_late,
  COALESCE(
    SAFE_CAST(t.is_delivery AS BOOL),
    SAFE_CAST(t.is_delivery AS INT64) != 0
  ) AS is_delivery,

  -- Primary source: raw_transaction.is_express through idx.
  -- Fallbacks only use fields that exist in raw_transaction_detail.
  CASE
    WHEN idx.is_express IS NOT NULL THEN idx.is_express
    WHEN t.company_outlet_express_service_id IS NOT NULL THEN TRUE
    WHEN COALESCE(
      SAFE_CAST(t.regular.express_price AS NUMERIC),
      0
    ) > 0 THEN TRUE
    ELSE FALSE
  END AS is_express,

  COALESCE(
    idx.express_service_id,
    CAST(t.company_outlet_express_service_id AS STRING)
  ) AS express_service_id,

  SAFE_CAST(t.transaction_progress AS INT64) AS transaction_progress,
  SAFE_CAST(t.workshop_progress AS INT64) AS workshop_progress,

  SAFE_CAST(t.created_at AS TIMESTAMP) AS transaction_created_at_utc,
  DATETIME(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_created_datetime_jakarta,
  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_date,

  SAFE_CAST(t.estimation_finish_at AS TIMESTAMP)
    AS estimation_finish_at_utc,
  SAFE_CAST(t.request_cancelled_at AS TIMESTAMP)
    AS request_cancelled_at_utc,
  SAFE_CAST(t.taking_at AS TIMESTAMP) AS taking_at_utc,
  SAFE_CAST(t.cancelled_at AS TIMESTAMP) AS cancelled_at_utc,

  CAST(t.request_cancelled_reason AS STRING)
    AS request_cancelled_reason,

  CAST(t.regular.payment_type AS STRING) AS payment_type,
  SAFE_CAST(t.regular.amount AS NUMERIC) AS regular_amount,
  SAFE_CAST(t.regular.net_amount AS NUMERIC) AS net_amount,
  SAFE_CAST(t.regular.net_amount_final AS NUMERIC)
    AS net_amount_final,
  SAFE_CAST(t.regular.gross_income AS NUMERIC) AS gross_income,

  SAFE_CAST(t.regular.discount AS NUMERIC)
    AS transaction_discount_amount,
  SAFE_CAST(t.regular.discount_regular_services AS NUMERIC)
    AS service_discount_amount,
  SAFE_CAST(t.regular.ppn AS NUMERIC) AS tax_amount,
  SAFE_CAST(t.regular.additional_price_amount AS NUMERIC)
    AS additional_price_amount,
  SAFE_CAST(t.regular.coin AS NUMERIC) AS coin_amount,

  SAFE_CAST(t.regular.amount_paid AS NUMERIC) AS amount_paid,
  SAFE_CAST(t.regular.amount_remaining AS NUMERIC)
    AS amount_remaining,

  SAFE_CAST(t.regular.normal_price AS NUMERIC) AS normal_price,
  SAFE_CAST(t.regular.express_price AS NUMERIC) AS express_price,

  SAFE_CAST(t.updated_at AS TIMESTAMP) AS source_updated_at_utc,
  idx.source_updated_at_utc AS source_list_updated_at_utc,

  idx.transaction_id IS NOT NULL AS is_found_in_transaction_index

FROM
  `{{ params.project_id }}.{{ params.raw_dataset }}.raw_transaction_detail` AS t

LEFT JOIN
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_transaction_index` AS idx
  ON CAST(t.id AS STRING) = idx.transaction_id

WHERE LOWER(CAST(t.transaction_type AS STRING)) = 'regular'
  AND t.regular.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(t.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(t._ingested_at AS TIMESTAMP) DESC
) = 1;
