-- Grain: one row per package transaction / deposit redemption.
--
-- Source of truth:
-- raw_transaction_detail
--
-- Enrichment only:
-- stg_transaction_index
--
-- raw_transaction_detail does not contain nested cashier.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_package_transactions`
AS

SELECT
  CAST(t.id AS STRING) AS package_transaction_id,
  CAST(t.package.id AS STRING) AS package_detail_id,

  CAST(t.note_number AS STRING) AS note_number,
  CAST(t.ref_id AS STRING) AS ref_id,

  SAFE_CAST(t.company_id AS INT64) AS company_id,
  SAFE_CAST(t.company_customer_id AS INT64) AS customer_source_id,

  CAST(t.customer.uuid AS STRING) AS customer_uuid,
  CAST(t.customer.name AS STRING) AS customer_name,
  SAFE_CAST(t.customer.is_membership_deposit AS BOOL)
    AS is_membership_deposit,

  SAFE_CAST(t.company_outlet_id AS INT64) AS outlet_id,
  CAST(t.outlet.name AS STRING) AS outlet_name,

  SAFE_CAST(t.user_employee_id AS INT64) AS employee_id,
  idx.employee_name,

  LOWER(CAST(t.transaction_type AS STRING)) AS transaction_type,
  LOWER(CAST(t.payment_status AS STRING)) AS payment_status,
  LOWER(CAST(t.transaction_status AS STRING)) AS transaction_status,
  CAST(t.transaction_status_text AS STRING) AS transaction_status_text,
  CAST(t.position AS STRING) AS position_name,

  SAFE_CAST(t.is_late AS BOOL) AS is_late,
  SAFE_CAST(t.is_delivery AS BOOL) AS is_delivery,

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

  SAFE_CAST(t.package.amount AS NUMERIC) AS package_amount,
  SAFE_CAST(t.package.net_amount AS NUMERIC) AS package_net_amount,
  SAFE_CAST(t.package.ppn AS NUMERIC) AS package_tax_amount,
  SAFE_CAST(t.package.coin AS NUMERIC) AS package_coin,

  SAFE_CAST(t.updated_at AS TIMESTAMP) AS source_updated_at_utc,
  idx.source_updated_at_utc AS source_list_updated_at_utc,

  idx.transaction_id IS NOT NULL AS is_found_in_transaction_index

FROM
  `{{ params.project_id }}.{{ params.raw_dataset }}.raw_transaction_detail` AS t

LEFT JOIN
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_transaction_index` AS idx
  ON CAST(t.id AS STRING) = idx.transaction_id

WHERE LOWER(CAST(t.transaction_type AS STRING)) = 'package'
  AND t.package.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(t.id AS STRING)
  ORDER BY
    SAFE_CAST(t.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(t._ingested_at AS TIMESTAMP) DESC
) = 1;
