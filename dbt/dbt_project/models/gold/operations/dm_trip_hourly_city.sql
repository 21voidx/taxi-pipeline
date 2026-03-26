-- ============================================================
-- gold_operations.dm_trip_hourly_city
-- Data mart: pola permintaan trip per jam per kota
-- Grain: 1 baris = 1 tanggal × 1 jam × 1 kota
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'   : 'date_key',
            'data_type': 'int64',
            'range'   : {'start': 20260101, 'end': 20991231, 'interval': 100}
        },
        cluster_by   = ['city', 'time_key']
    )
}}

WITH trips AS (
    SELECT *
    FROM {{ ref('fct_trip') }}

    {% if is_incremental() %}
    WHERE request_date_key >= CAST(
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
    t.request_date_key                                          AS date_key,
    t.request_time_key                                          AS time_key,
    tm.time_bucket,
    tm.is_peak_hour,
    t.city,

    -- Volume
    COUNT(*)                                                    AS total_trip_requested,
    COUNTIF(t.is_completed)                                     AS total_trip_completed,
    COUNTIF(t.is_cancelled)                                     AS total_trip_cancelled,

    -- Surge
    ROUND(AVG(t.surge_multiplier), 3)                           AS avg_surge_multiplier,
    MAX(t.surge_multiplier)                                     AS max_surge_multiplier,
    COUNTIF(t.has_surge)                                        AS surge_trip_count,

    -- Metrics
    {{ safe_div('COUNTIF(t.is_completed)', 'COUNT(*)') }}       AS completion_rate,
    ROUND(AVG(t.waiting_time_minute), 1)                        AS avg_waiting_time_minute,
    ROUND(AVG(CASE WHEN t.is_completed THEN t.actual_fare END), 0) AS avg_actual_fare

FROM trips t
LEFT JOIN {{ ref('dim_time') }} tm ON t.request_time_key = tm.time_key

GROUP BY
    t.request_date_key,
    t.request_time_key,
    tm.time_bucket,
    tm.is_peak_hour,
    t.city
