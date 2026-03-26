-- ============================================================
-- gold_operations.dm_trip_daily_city
-- Data mart operasional: kinerja trip harian per kota
-- Grain: 1 baris = 1 tanggal × 1 kota
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'   : 'date_key',
            'data_type': 'int64',
            'range'   : {'start': 20260101, 'end': 20991231, 'interval': 100}
        },
        cluster_by   = ['city']
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
    t.request_date_key                              AS date_key,
    t.city,

    -- Volume
    COUNT(*)                                        AS total_trip_requested,
    COUNTIF(t.is_completed)                         AS total_trip_completed,
    COUNTIF(t.is_cancelled)                         AS total_trip_cancelled,
    COUNTIF(t.trip_status = 'no_show')              AS total_trip_no_show,

    -- Rate
    {{ safe_div('COUNTIF(t.is_completed)', 'COUNT(*)') }}    AS completion_rate,
    {{ safe_div('COUNTIF(t.is_cancelled)', 'COUNT(*)') }}    AS cancellation_rate,

    -- Jarak
    ROUND(AVG(CASE WHEN t.is_completed THEN t.estimated_distance_km END), 2)  AS avg_estimated_distance_km,
    ROUND(AVG(CASE WHEN t.is_completed THEN t.actual_distance_km    END), 2)  AS avg_actual_distance_km,

    -- Waktu
    ROUND(AVG(CASE WHEN t.is_completed THEN t.trip_duration_minute  END), 1)  AS avg_trip_duration_minute,
    ROUND(AVG(t.waiting_time_minute), 1)                                       AS avg_waiting_time_minute,

    -- Fare (IDR)
    ROUND(AVG(CASE WHEN t.is_completed THEN t.actual_fare           END), 0)  AS avg_actual_fare,
    SUM(CASE WHEN t.is_completed THEN t.actual_fare ELSE 0 END)               AS total_actual_fare,

    -- Surge
    ROUND(AVG(t.surge_multiplier), 3)               AS avg_surge_multiplier,
    COUNTIF(t.has_surge)                            AS total_surge_trips,

    -- Vehicle type breakdown
    COUNTIF(t.is_completed AND v.vehicle_type = 'economy')  AS completed_economy,
    COUNTIF(t.is_completed AND v.vehicle_type = 'standard') AS completed_standard,
    COUNTIF(t.is_completed AND v.vehicle_type = 'premium')  AS completed_premium,
    COUNTIF(t.is_completed AND v.vehicle_type = 'motor')    AS completed_motor

FROM trips t
LEFT JOIN {{ ref('dim_vehicle') }} v ON t.vehicle_key = v.vehicle_key

GROUP BY
    t.request_date_key,
    t.city
