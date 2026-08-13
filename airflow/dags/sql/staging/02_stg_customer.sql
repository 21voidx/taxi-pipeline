-- Grain: satu row = satu customer.
-- Raw diasumsikan sudah berupa satu customer object per NDJSON row.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_customer`
AS

SELECT
  CAST(customer_id AS STRING) AS customer_uuid,
  NULLIF(TRIM(CAST(customer_name AS STRING)), '') AS customer_name,
  NULLIF(TRIM(CAST(customer_code AS STRING)), '') AS customer_code,
  NULLIF(TRIM(CAST(honorific AS STRING)), '') AS honorific,

  COALESCE(
    SAFE_CAST(is_membership AS BOOL),
    SAFE_CAST(is_membership AS INT64) != 0
  ) AS is_membership,

  COALESCE(
    SAFE_CAST(is_membership_deposit AS BOOL),
    SAFE_CAST(is_membership_deposit AS INT64) != 0
  ) AS is_membership_deposit,

  COALESCE(
    SAFE_CAST(is_active AS BOOL),
    SAFE_CAST(is_active AS INT64) != 0
  ) AS is_active,

  NULLIF(TRIM(CAST(phone AS STRING)), '') AS phone_raw,
  SAFE_CAST(gender AS INT64) AS gender_code,

  SAFE_CAST(customer_created_date AS DATETIME)
    AS customer_created_datetime_jakarta,

  SAFE_CAST(total_transaction_regular AS INT64)
    AS source_total_transaction_regular,

  SAFE_CAST(total_transaction_self_service AS INT64)
    AS source_total_transaction_self_service,

  detail_customer_addresses,
  detail_customer_outlets

FROM `{{ params.project_id }}.{{ params.raw_dataset }}.raw_customer`

WHERE customer_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(customer_id AS STRING)
  ORDER BY SAFE_CAST(_ingested_at AS TIMESTAMP) DESC
) = 1;

-- Snapshot customer dapat di-append berulang.
-- Dedup di atas memilih snapshot terbaru berdasarkan _ingested_at.
