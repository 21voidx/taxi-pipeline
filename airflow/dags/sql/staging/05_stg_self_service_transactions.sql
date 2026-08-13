-- Grain: one row per self-service transaction.
--
-- Verified raw fields:
-- total, status, transaction_type, company_outlet, payment,
-- fee_amount, total_paid, services.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_transactions`
AS

SELECT
  CAST(t.id AS STRING) AS self_service_transaction_id,
  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.ref_id AS STRING) AS ref_id,

  SAFE_CAST(t.company_id AS INT64) AS company_id,
  SAFE_CAST(t.company_customer_id AS INT64) AS customer_source_id,

  -- The uploaded example has customer = NULL.
  -- Keep customer UUID/name NULL until the source consistently supplies them.
  CAST(NULL AS STRING) AS customer_uuid,
  CAST(NULL AS STRING) AS customer_name,

  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,
  CAST(t.company_outlet.name AS STRING) AS outlet_name,

  CAST(t.iot_machine_qr_code_id AS STRING)
    AS iot_machine_qr_code_id,

  LOWER(CAST(t.status AS STRING)) AS payment_status,
  LOWER(CAST(t.transaction_type AS STRING))
    AS self_service_transaction_type,

  COALESCE(
    SAFE_CAST(t.total AS NUMERIC),
    SAFE_CAST(t.payment.amount AS NUMERIC)
  ) AS customer_charged_amount,

  SAFE_CAST(t.fee_amount AS NUMERIC) AS payment_fee_amount,
  SAFE_CAST(t.total_paid AS NUMERIC) AS merchant_net_amount,

  CAST(t.payment.id AS STRING) AS payment_id,
  CAST(t.payment.channel_payment AS STRING) AS payment_channel,
  CAST(t.payment.brand_name AS STRING) AS payment_brand,
  CAST(t.payment.currency AS STRING) AS payment_currency,
  CAST(t.payment.rrn AS STRING) AS payment_rrn,

  TO_HEX(
    SHA256(COALESCE(CAST(t.payment.buyer_ref AS STRING), ''))
  ) AS buyer_ref_hash,

  SAFE_CAST(t.finished_at AS DATETIME)
    AS finished_datetime_jakarta,

  TIMESTAMP(
    SAFE_CAST(t.finished_at AS DATETIME),
    'Asia/Jakarta'
  ) AS finished_at_utc,

  SAFE_CAST(t.created_at AS TIMESTAMP)
    AS transaction_created_at_utc,

  DATETIME(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_created_datetime_jakarta,

  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_date,

  SAFE_CAST(t.updated_at AS TIMESTAMP)
    AS source_updated_at_utc

FROM
  `{{ params.project_id }}.{{ params.raw_dataset }}.raw_transaction_self_service` AS t

WHERE t.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(t.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(t._ingested_at AS TIMESTAMP) DESC
) = 1;
