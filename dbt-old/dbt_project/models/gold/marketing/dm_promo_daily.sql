-- ============================================================
-- gold_marketing.dm_promo_daily
-- Data mart: efektivitas promo per hari
-- Grain: 1 baris = 1 tanggal × 1 promo_code
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'    : 'date_key',
            'data_type': 'int64',
            'range'    : {'start': 20260101, 'end': 20991231, 'interval': 100}
        },
        cluster_by   = ['promo_code']
    )
}}

WITH redemptions AS (
    SELECT
        r.redemption_date_key,
        r.promo_key,
        r.trip_key,
        r.customer_key,
        r.is_success,
        r.discount_amount
    FROM {{ ref('fct_promo_redemption') }} r

    {% if is_incremental() %}
    WHERE r.redemption_date_key >= CAST(
        FORMAT_DATE('%Y%m%d',
            DATE_SUB(
                PARSE_DATE('%Y%m%d', CAST(
                    (SELECT MAX(date_key) FROM {{ this }})
                    AS STRING)),
                INTERVAL {{ var('incremental_lookback_days', 3) }} DAY
            )
        ) AS INT64)
    {% endif %}
),

trips AS (
    SELECT trip_key, is_completed
    FROM {{ ref('fct_trip') }}
),

promos AS (
    SELECT promo_key, promo_id, promo_code, promo_name,
           promo_type, discount_type, discount_value
    FROM {{ ref('dim_promo') }}
)

SELECT
    r.redemption_date_key                               AS date_key,
    p.promo_code,
    p.promo_name,
    p.promo_type,
    p.discount_type,
    p.discount_value,

    -- Volume
    COUNT(*)                                            AS total_redemption,
    COUNTIF(r.is_success)                               AS successful_redemption,
    COUNT(DISTINCT r.customer_key)                      AS unique_customer_count,
    COUNT(DISTINCT r.trip_key)                          AS total_trip_count,
    COUNTIF(t.is_completed)                             AS completed_trip_count,

    -- Value
    SUM(CASE WHEN r.is_success THEN r.discount_amount ELSE 0 END)
                                                        AS total_discount_amount,
    {{ safe_div(
        'SUM(CASE WHEN r.is_success THEN r.discount_amount ELSE 0 END)',
        'COUNTIF(r.is_success)'
    ) }}                                                AS avg_discount_per_redemption,

    -- Conversion
    {{ safe_div('COUNTIF(t.is_completed)', 'COUNT(*)') }}
                                                        AS redemption_to_completion_rate

FROM redemptions r
LEFT JOIN trips  t ON r.trip_key  = t.trip_key
LEFT JOIN promos p ON r.promo_key = p.promo_key

GROUP BY
    r.redemption_date_key,
    p.promo_code,
    p.promo_name,
    p.promo_type,
    p.discount_type,
    p.discount_value
