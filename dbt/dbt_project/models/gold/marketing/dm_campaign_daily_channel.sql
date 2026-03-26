-- ============================================================
-- gold_marketing.dm_campaign_daily_channel
-- Data mart: pengeluaran & performa campaign per channel per hari
-- Grain: 1 baris = 1 tanggal × 1 channel × 1 campaign
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'    : 'date_key',
            'data_type': 'int64',
            'range'    : {'start': 20260101, 'end': 20991231, 'interval': 100}
        },
        cluster_by   = ['channel_name']
    )
}}

WITH campaign_raw AS (
    SELECT *
    FROM {{ source('bronze_mysql', 'campaign_spend_raw') }}

    {% if is_incremental() %}
    WHERE CAST(FORMAT_DATE('%Y%m%d', spend_date) AS INT64) >= CAST(
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
    CAST(FORMAT_DATE('%Y%m%d', spend_date) AS INT64)    AS date_key,
    spend_date,
    channel_name,
    campaign_name,

    -- Spend
    CAST(SUM(spend_amount) AS NUMERIC)                   AS spend_amount,

    -- Volume
    SUM(impressions)                                     AS impressions,
    SUM(clicks)                                          AS clicks,
    SUM(installs)                                        AS installs,

    -- Rates
    {{ safe_div('CAST(SUM(clicks) AS FLOAT64)', 'SUM(impressions)') }}
                                                         AS ctr,           -- click-through rate
    {{ safe_div('CAST(SUM(installs) AS FLOAT64)', 'SUM(clicks)') }}
                                                         AS click_to_install_rate,

    -- Cost metrics (IDR)
    {{ safe_div('CAST(SUM(spend_amount) AS FLOAT64)', 'SUM(impressions)') }}
                                                         AS cpm,           -- cost per mille
    {{ safe_div('CAST(SUM(spend_amount) AS FLOAT64)', 'SUM(clicks)') }}
                                                         AS cpc,           -- cost per click
    CAST({{ safe_div('CAST(SUM(spend_amount) AS FLOAT64)', 'SUM(installs)') }} AS NUMERIC)
                                                         AS cpi            -- cost per install

FROM campaign_raw
GROUP BY
    spend_date,
    channel_name,
    campaign_name
