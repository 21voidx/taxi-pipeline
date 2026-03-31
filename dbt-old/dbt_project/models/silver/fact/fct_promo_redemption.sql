-- ============================================================
-- silver_core.fct_promo_redemption
-- Fact pemakaian promo per trip
-- Grain: 1 baris = 1 redemption
-- Materialization: incremental (merge by redemption_id)
-- ============================================================

{{
    config(
        unique_key       = 'redemption_id',
        partition_by     = {
            'field'      : 'created_at',
            'data_type'  : 'timestamp',
            'granularity': 'day'
        },
        cluster_by       = ['redemption_status']
    )
}}

WITH redemptions AS (
    SELECT *
    FROM {{ source('bronze_mysql', 'promo_redemptions_raw') }}
    {{ incremental_filter('updated_at') }}
),

dim_promo AS (
    SELECT promo_key, promo_id
    FROM {{ ref('dim_promo') }}
),

dim_trip AS (
    SELECT trip_key, trip_id
    FROM {{ ref('fct_trip') }}
),

dim_customer AS (
    SELECT customer_key, customer_id
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
)

SELECT
    {{ surrogate_key(['r.redemption_id']) }}     AS promo_redemption_key,
    r.redemption_id,
    dp.promo_key,
    dt.trip_key,
    dc.customer_key,

    -- Date key
    {{ date_to_key('r.redeemed_ts') }}           AS redemption_date_key,

    -- Degenerate dimension
    r.redemption_status,

    -- Measure
    CAST(r.discount_amount AS NUMERIC)           AS discount_amount,

    -- Flags
    (r.redemption_status = 'success')            AS is_success,

    r.redeemed_ts,
    r.created_at,
    r.updated_at

FROM redemptions r
LEFT JOIN dim_promo    dp ON r.promo_id    = dp.promo_id
LEFT JOIN dim_trip     dt ON r.trip_id     = dt.trip_id
LEFT JOIN dim_customer dc ON r.customer_id = dc.customer_id
