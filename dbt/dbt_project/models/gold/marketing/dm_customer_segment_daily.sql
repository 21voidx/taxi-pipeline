-- ============================================================
-- gold_marketing.dm_customer_segment_daily
-- Data mart: distribusi segmen customer per hari (dari snapshot CRM)
-- Grain: 1 baris = 1 snapshot_date × 1 segment_name
-- Materialization: incremental merge (unique by snapshot_date + segment)
-- ============================================================

{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = ['snapshot_date', 'segment_name']
    )
}}

WITH segments AS (
    SELECT *
    FROM {{ source('bronze_mysql', 'customer_segments_raw') }}

    {% if is_incremental() %}
    WHERE snapshot_date >= DATE_SUB(
        (SELECT MAX(snapshot_date) FROM {{ this }}),
        INTERVAL {{ var('incremental_lookback_days', 3) }} DAY
    )
    {% endif %}
),

-- Pisahkan agregasi biasa dari window function
aggregated AS (
    SELECT
        snapshot_date,
        segment_name,
        COUNT(DISTINCT customer_id)        AS customer_count,
        ROUND(AVG(segment_score), 2)       AS avg_segment_score,
        ROUND(MIN(segment_score), 2)       AS min_segment_score,
        ROUND(MAX(segment_score), 2)       AS max_segment_score,
        ROUND(STDDEV(segment_score), 2)    AS stddev_segment_score
    FROM segments
    GROUP BY snapshot_date, segment_name
),

-- Window function di CTE terpisah
with_median AS (
    SELECT
        *,
        ROUND(PERCENTILE_CONT(avg_segment_score, 0.5)
            OVER (PARTITION BY snapshot_date, segment_name), 2)
                                           AS median_segment_score
    FROM aggregated
)

SELECT * FROM with_median