-- ============================================================
-- gold_finance.dm_payment_method_daily
-- Data mart: penggunaan metode pembayaran per hari
-- Grain: 1 baris = 1 tanggal × 1 metode pembayaran
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'    : 'date_key',
            'data_type': 'int64',
            'range'    : {'start': 20260101, 'end': 20991231, 'interval': 100}
        },
        cluster_by   = ['payment_method_name']
    )
}}

WITH payments AS (
    SELECT
        p.payment_date_key,
        pm.payment_method_name,
        pm.payment_method_category,
        pm.is_digital_wallet,
        p.payment_status,
        p.is_success,
        p.net_amount,
        p.discount_amount,
        p.has_discount
    FROM {{ ref('fct_payment') }} p
    LEFT JOIN {{ ref('dim_payment_method') }} pm
        ON p.payment_method_key = pm.payment_method_key

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
)

SELECT
    payment_date_key                                AS date_key,
    payment_method_name,
    payment_method_category,
    is_digital_wallet,

    -- Volume
    COUNT(*)                                        AS payment_count,
    COUNTIF(is_success)                             AS success_payment_count,
    COUNTIF(NOT is_success)                         AS failed_payment_count,

    -- Revenue
    SUM(CASE WHEN is_success THEN net_amount ELSE 0 END)      AS total_net_amount,
    {{ safe_div(
        'SUM(CASE WHEN is_success THEN net_amount ELSE 0 END)',
        'COUNTIF(is_success)'
    ) }}                                            AS avg_net_amount,

    -- Discount
    SUM(CASE WHEN is_success THEN discount_amount ELSE 0 END) AS total_discount_amount,
    COUNTIF(is_success AND has_discount)            AS discounted_payment_count,

    -- Success rate
    {{ safe_div('COUNTIF(is_success)', 'COUNT(*)') }} AS success_rate

FROM payments
GROUP BY
    payment_date_key,
    payment_method_name,
    payment_method_category,
    is_digital_wallet
