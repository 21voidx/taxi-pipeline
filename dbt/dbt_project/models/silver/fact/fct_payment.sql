-- ============================================================
-- silver_core.fct_payment
-- Fact transaksi pembayaran
-- Grain: 1 baris = 1 payment per trip
-- Materialization: incremental (merge by payment_id)
-- ============================================================

{{
    config(
        unique_key       = 'payment_id',
        partition_by     = {
            'field'      : 'created_at',
            'data_type'  : 'timestamp',
            'granularity': 'day'
        },
        cluster_by       = ['payment_method', 'payment_status']
    )
}}

WITH payments AS (
    SELECT *
    FROM {{ source('bronze_pg', 'payments_raw') }}
    {{ incremental_filter('updated_at') }}
),

dim_trip AS (
    SELECT trip_key, trip_id, city
    FROM {{ ref('fct_trip') }}
),

dim_customer AS (
    SELECT customer_key, customer_id
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

dim_payment_method AS (
    SELECT payment_method_key, payment_method_name
    FROM {{ ref('dim_payment_method') }}
)

SELECT
    {{ surrogate_key(['p.payment_id']) }}              AS payment_key,
    p.payment_id,
    dt.trip_key,
    dc.customer_key,

    -- Date key dari paid_ts
    {{ date_to_key('p.paid_ts') }}                     AS payment_date_key,

    -- FK metode bayar
    COALESCE(dpm.payment_method_key,
        (SELECT payment_method_key FROM dim_payment_method WHERE payment_method_name = 'unknown')
    )                                                  AS payment_method_key,

    -- Degenerate dimensions
    p.payment_method,
    p.payment_status,

    -- Measures (IDR)
    CAST(p.gross_amount    AS NUMERIC)                 AS gross_amount,
    CAST(p.discount_amount AS NUMERIC)                 AS discount_amount,
    CAST(p.tax_amount      AS NUMERIC)                 AS tax_amount,
    CAST(p.toll_amount     AS NUMERIC)                 AS toll_amount,
    CAST(p.tip_amount      AS NUMERIC)                 AS tip_amount,
    CAST(p.net_amount      AS NUMERIC)                 AS net_amount,

    -- Derived measures
    CAST(p.gross_amount - p.discount_amount AS NUMERIC) AS discounted_amount,

    -- Flags
    (p.payment_status = 'success')                    AS is_success,
    (p.discount_amount > 0)                           AS has_discount,
    (p.toll_amount     > 0)                           AS has_toll,
    (p.tip_amount      > 0)                           AS has_tip,

    p.paid_ts,
    p.created_at,
    p.updated_at

FROM payments p
LEFT JOIN dim_trip           dt  ON p.trip_id   = dt.trip_id
LEFT JOIN dim_customer       dc  ON p.customer_id = dc.customer_id
LEFT JOIN dim_payment_method dpm ON LOWER(p.payment_method) = dpm.payment_method_name
