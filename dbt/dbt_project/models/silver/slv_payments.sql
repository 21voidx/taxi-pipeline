{{
  config(
    materialized        = 'incremental',
    unique_key          = 'payment_id',
    incremental_strategy= 'merge',
    partition_by        = {
      'field': 'payment_date',
      'data_type': 'date'
    },
    cluster_by          = ['payment_status', 'method_type'],
    tags                = ['silver', 'mysql', 'json_parsed']
  )
}}

/*
  SILVER: slv_payments
  ══════════════════════════════════════════════════════════════
  Applies:
    ✓ JSON   – parse gateway_raw_response → device_type, txn_ref, response_code
    ✓ REGEX  – validate gateway_ref format (TXN + 16 digits)
    ✓ JOIN   – enrich dengan payment_methods (method_type, provider)
    ✓ DERIVE – is_ewallet, is_cash, is_card, is_refunded,
               processing_lag_seconds
*/

WITH raw AS (
    SELECT * FROM {{ ref('brz_payments') }}
),

payment_methods AS (
    SELECT
        method_id,
        method_code,
        method_name,
        method_type,
        provider
    FROM {{ source('mysql_raw', 'payment_methods') }}
),

parsed AS (
    SELECT
        p.payment_id,
        p.payment_code,
        p.ride_id,
        p.passenger_id,
        p.method_id,
        pm.method_code,
        pm.method_name,
        pm.method_type,
        pm.provider                                      AS payment_provider,

        -- ── Amounts ──────────────────────────────────────────────
        p.amount,
        COALESCE(p.platform_fee, p.amount * 0.20)       AS platform_fee,
        COALESCE(p.driver_earning, p.amount * 0.80)     AS driver_earning,
        COALESCE(p.promo_discount, 0)                   AS promo_discount,
        p.currency,

        -- ── Status ───────────────────────────────────────────────
        UPPER(p.payment_status)                         AS payment_status,
        p.payment_gateway,

        -- ── Gateway ref REGEX validation ─────────────────────────
        p.gateway_ref,
        REGEXP_CONTAINS(
          COALESCE(p.gateway_ref, ''),
          r'^TXN\d{16}$'
        )                                               AS is_gateway_ref_valid,

        -- ── JSON parsing dari gateway_raw_response ───────────────
        JSON_VALUE(p.gateway_raw_response, '$.device_type')
                                                        AS device_type,
        JSON_VALUE(p.gateway_raw_response, '$.response_code')
                                                        AS gateway_response_code,
        JSON_VALUE(p.gateway_raw_response, '$.txn_ref')
                                                        AS gateway_txn_ref,

        -- ── Derived flags ────────────────────────────────────────
        (pm.method_type = 'EWALLET')                    AS is_ewallet,
        (pm.method_type = 'CASH')                       AS is_cash,
        (pm.method_type IN ('CARD', 'BANK'))            AS is_card,
        (p.payment_status = 'REFUNDED')                 AS is_refunded,

        -- ── Timing ───────────────────────────────────────────────
        DATE(p.created_at)                              AS payment_date,
        p.paid_at,
        p.expired_at,
        p.refunded_at,
        p.refund_reason,

        -- Processing lag (detik dari created ke paid)
        TIMESTAMP_DIFF(p.paid_at, p.created_at, SECOND) AS processing_lag_seconds,

        p.created_at,
        p.updated_at,
        p._loaded_at,
        p._source_table,
        p._row_hash

    FROM raw p
    LEFT JOIN payment_methods pm ON p.method_id = pm.method_id
)

SELECT *
FROM parsed
{% if is_incremental() %}
WHERE updated_at > (
    SELECT TIMESTAMP_SUB(MAX(updated_at), INTERVAL 1 HOUR) FROM {{ this }}
)
{% endif %}
