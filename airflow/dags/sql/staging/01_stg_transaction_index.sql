-- Grain: one row per transaction from GET /transactions.
--
-- Source: raw_transaction (list endpoint)
--
-- Fields intentionally sourced here:
-- - cashier / employee name
-- - is_express
-- - list/index status
--
-- This model is an extraction control and enrichment source.
-- It is NOT the source of truth for revenue.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_transaction_index`
AS

SELECT
  CAST(t.id AS STRING) AS transaction_id,
  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.ref_id AS STRING) AS ref_id,

  SAFE_CAST(t.company_id AS INT64) AS company_id,
  SAFE_CAST(t.company_customer_id AS INT64) AS customer_source_id,

  CAST(t.customer.uuid AS STRING) AS customer_uuid,
  CAST(t.customer.name AS STRING) AS customer_name,

  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,
  CAST(t.outlet.name AS STRING) AS outlet_name,

  COALESCE(
    SAFE_CAST(t.cashier.id AS INT64),
    SAFE_CAST(t.user_employee_id AS INT64)
  ) AS employee_id,

  CAST(t.cashier.name AS STRING) AS employee_name,

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

  -- is_express exists in raw_transaction, not raw_transaction_detail.
  COALESCE(
    SAFE_CAST(t.is_express AS BOOL),
    SAFE_CAST(t.is_express AS INT64) != 0
  ) AS is_express,

  SAFE_CAST(t.company_outlet_express_service_id AS STRING)
    AS express_service_id,

  SAFE_CAST(t.transaction_progress AS INT64) AS transaction_progress,
  SAFE_CAST(t.workshop_progress AS INT64) AS workshop_progress,

  SAFE_CAST(t.estimation_finish_at AS TIMESTAMP)
    AS estimation_finish_at_utc,
  SAFE_CAST(t.created_at AS TIMESTAMP) AS transaction_created_at_utc,
  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_date,
  SAFE_CAST(t.updated_at AS TIMESTAMP) AS source_updated_at_utc

FROM
  `{{ params.project_id }}.{{ params.raw_dataset }}.raw_transaction` AS t

WHERE t.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(t.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(t._ingested_at AS TIMESTAMP) DESC
) = 1;
