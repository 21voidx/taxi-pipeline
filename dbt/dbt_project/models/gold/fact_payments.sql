{{
  config(
    materialized        = 'incremental',
    unique_key          = 'payment_id',
    incremental_strategy= 'merge',
    partition_by        = {'field': 'payment_date', 'data_type': 'date'},
    cluster_by          = ['payment_status', 'method_type'],
    tags                = ['gold', 'fact']
  )
}}

/*
  GOLD FACT: fact_payments
  ══════════════════════════════════════════════════════════════
  Grain: satu baris per transaksi pembayaran.
  Digunakan oleh mart_executive_summary untuk refund metrics.
*/

WITH payments AS (
    SELECT * FROM {{ ref('slv_payments') }}
    {% if is_incremental() %}
    WHERE updated_at > (
        SELECT TIMESTAMP_SUB(MAX(updated_at), INTERVAL 1 HOUR) FROM {{ this }}
    )
    {% endif %}
),

dim_payment_method AS (
    SELECT method_id, payment_method_sk
    FROM {{ ref('dim_payment_method') }}
),

dim_date AS (
    SELECT full_date, date_key
    FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['p.payment_id']) }}    AS payment_sk,
    p.payment_id,
    p.payment_code,
    p.ride_id,
    p.passenger_id,

    -- ── Foreign keys ──────────────────────────────────────────────
    dd.date_key                                         AS payment_date_key,
    COALESCE(pm.payment_method_sk, 'UNKNOWN')           AS payment_method_sk,

    -- ── Degenerate dims ───────────────────────────────────────────
    p.payment_status,
    p.payment_gateway,
    p.method_type,
    p.payment_provider,
    p.device_type,
    p.gateway_response_code,
    p.is_gateway_ref_valid,

    -- ── Boolean flags ─────────────────────────────────────────────
    p.is_ewallet,
    p.is_cash,
    p.is_card,
    p.is_refunded,

    -- ── Measures ─────────────────────────────────────────────────
    p.amount,
    p.platform_fee,
    p.driver_earning,
    p.promo_discount,
    CAST(p.is_refunded AS INT64)                        AS refund_count,
    CASE WHEN p.is_refunded THEN p.amount ELSE 0 END    AS refund_amount,
    p.processing_lag_seconds,

    -- ── Timestamps ───────────────────────────────────────────────
    p.payment_date,
    p.paid_at,
    p.expired_at,
    p.refunded_at,
    p.refund_reason,
    p.created_at,
    p.updated_at

FROM payments p
LEFT JOIN dim_payment_method pm ON p.method_id    = pm.method_id
LEFT JOIN dim_date           dd ON p.payment_date  = dd.full_date
