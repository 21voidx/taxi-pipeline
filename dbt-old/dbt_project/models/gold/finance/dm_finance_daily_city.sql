-- ============================================================
-- gold_finance.dm_finance_daily_city
-- Data mart keuangan harian per kota
-- Grain: 1 baris = 1 tanggal × 1 kota
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'    : 'date_key',
            'data_type': 'int64',
            'range'    : {'start': 20260101, 'end': 20991231, 'interval': 100}
        },
        cluster_by   = ['city']
    )
}}

WITH payments AS (
    SELECT
        p.*,
        t.city,
        t.request_date_key
    FROM {{ ref('fct_payment') }} p
    JOIN {{ ref('fct_trip') }}    t ON p.trip_key = t.trip_key

    {% if is_incremental() %}
    WHERE p.payment_date_key >= CAST(
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

payouts AS (
    SELECT
        dp.*,
        t.city
    FROM {{ ref('fct_driver_payout') }} dp
    JOIN {{ ref('fct_trip') }}          t ON dp.trip_key = t.trip_key
),

payment_agg AS (
    SELECT
        payment_date_key                               AS date_key,
        city,
        COUNT(*)                                       AS total_payment_count,
        COUNTIF(is_success)                            AS success_payment_count,
        SUM(CASE WHEN is_success THEN gross_amount    ELSE 0 END) AS total_gross_amount,
        SUM(CASE WHEN is_success THEN discount_amount ELSE 0 END) AS total_discount_amount,
        SUM(CASE WHEN is_success THEN tax_amount      ELSE 0 END) AS total_tax_amount,
        SUM(CASE WHEN is_success THEN toll_amount     ELSE 0 END) AS total_toll_amount,
        SUM(CASE WHEN is_success THEN tip_amount      ELSE 0 END) AS total_tip_amount,
        SUM(CASE WHEN is_success THEN net_amount      ELSE 0 END) AS total_net_amount
    FROM payments
    GROUP BY payment_date_key, city
),

payout_agg AS (
    SELECT
        payout_date_key                                AS date_key,
        city,
        SUM(CASE WHEN is_paid THEN final_payout_amount ELSE 0 END) AS total_driver_payout,
        SUM(CASE WHEN is_paid THEN bonus_amount        ELSE 0 END) AS total_driver_bonus
    FROM payouts
    GROUP BY payout_date_key, city
)

SELECT
    pa.date_key,
    pa.city,

    -- Payment breakdown
    pa.total_payment_count,
    pa.success_payment_count,
    pa.total_gross_amount,
    pa.total_discount_amount,
    pa.total_tax_amount,
    pa.total_toll_amount,
    pa.total_tip_amount,
    pa.total_net_amount,

    -- Payout ke driver
    COALESCE(po.total_driver_payout, 0)               AS total_driver_payout,
    COALESCE(po.total_driver_bonus, 0)                AS total_driver_bonus,

    -- Platform revenue = net_amount − driver_payout
    pa.total_net_amount - COALESCE(po.total_driver_payout, 0)
                                                       AS estimated_platform_revenue,

    -- Avg per trip selesai
    {{ safe_div('pa.total_net_amount', 'pa.success_payment_count') }}
                                                       AS avg_revenue_per_trip,

    -- Discount rate
    {{ safe_div('pa.total_discount_amount', 'pa.total_gross_amount') }}
                                                       AS discount_rate,

    -- Take rate (platform %)
    {{ safe_div(
        'pa.total_net_amount - COALESCE(po.total_driver_payout, 0)',
        'pa.total_net_amount'
    ) }}                                               AS platform_take_rate

FROM payment_agg pa
LEFT JOIN payout_agg po
    ON  pa.date_key = po.date_key
    AND pa.city     = po.city
