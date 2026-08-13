-- Grain: one row per deposit purchase / deposit cash-in.
--
-- Verified raw fields:
-- amount, net_amount, ppn_amount, status, cashier, customer,
-- outlet, master_payment_method_id, deposit_services.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_deposit_purchases`
AS

SELECT
  CAST(t.id AS STRING) AS deposit_purchase_id,
  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.ref_id AS STRING) AS ref_id,

  SAFE_CAST(t.company_customer_id AS INT64)
    AS customer_source_id,
  CAST(t.customer.uuid AS STRING) AS customer_uuid,
  CAST(t.customer.name AS STRING) AS customer_name,

  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,
  CAST(t.outlet.name AS STRING) AS outlet_name,

  SAFE_CAST(t.cashier.id AS INT64) AS employee_id,
  CAST(t.cashier.name AS STRING) AS employee_name,

  SAFE_CAST(t.master_payment_method_id AS INT64)
    AS payment_method_id,

  -- The payload only supplies the payment method ID.
  CAST(NULL AS STRING) AS payment_method_name,

  LOWER(CAST(t.status AS STRING)) AS purchase_status,

  SAFE_CAST(t.amount AS NUMERIC) AS gross_amount,
  SAFE_CAST(t.net_amount AS NUMERIC) AS deposit_cash_in,
  SAFE_CAST(t.ppn_amount AS NUMERIC) AS tax_amount,

  SAFE_CAST(t.created_at AS TIMESTAMP)
    AS purchase_created_at_utc,

  DATETIME(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS purchase_created_datetime_jakarta,

  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS purchase_date,

  SAFE_CAST(t.request_cancelled_at AS TIMESTAMP)
    AS request_cancelled_at_utc,
  CAST(t.request_cancelled_reason AS STRING)
    AS request_cancelled_reason,

  SAFE_CAST(t.cancelled_at AS TIMESTAMP)
    AS cancelled_at_utc,

  SAFE_CAST(t.updated_at AS TIMESTAMP)
    AS source_updated_at_utc

FROM
  `{{ params.project_id }}.{{ params.raw_dataset }}.raw_deposit_purchase` AS t

WHERE t.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(t.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(t._ingested_at AS TIMESTAMP) DESC
) = 1;
