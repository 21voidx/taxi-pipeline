-- ============================================================
-- gold_driver.dm_driver_daily_performance
-- Data mart: performa driver per hari
-- Grain: 1 baris = 1 tanggal × 1 driver
-- Materialization: incremental insert_overwrite (partisi date_key)
-- ============================================================

{{
    config(
        partition_by = {
            'field'    : 'date_key',
            'data_type': 'int64',
            'range'    : {'start': 20260101, 'end': 20991231, 'interval': 1}
        },
        cluster_by   = ['city']
    )
}}

WITH trips AS (
    SELECT
        t.request_date_key,
        t.driver_key,
        t.city,
        t.is_completed,
        t.is_cancelled,
        t.trip_status,
        t.actual_distance_km,
        t.actual_fare,
        t.waiting_time_minute,
        t.trip_duration_minute
    FROM {{ ref('fct_trip') }} t

    {% if is_incremental() %}
    WHERE t.request_date_key >= CAST(
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

ratings AS (
    SELECT
        r.driver_key,
        r.rating_date_key,
        r.rating_score
    FROM {{ ref('fct_rating') }} r
),

payouts AS (
    SELECT
        dp.driver_key,
        dp.payout_date_key,
        dp.final_payout_amount,
        dp.is_paid
    FROM {{ ref('fct_driver_payout') }} dp
),

drivers AS (
    SELECT driver_key, driver_id, full_name
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
),

trip_agg AS (
    SELECT
        t.request_date_key                                  AS date_key,
        t.driver_key,
        t.city,
        COUNT(*)                                            AS total_trip_assigned,
        COUNTIF(t.is_completed)                             AS total_trip_completed,
        COUNTIF(t.is_cancelled)                             AS total_trip_cancelled,
        {{ safe_div('COUNTIF(t.is_completed)', 'COUNT(*)') }} AS completion_rate,
        ROUND(SUM(CASE WHEN t.is_completed THEN t.actual_distance_km ELSE 0 END), 2)
                                                            AS total_actual_distance_km,
        SUM(CASE WHEN t.is_completed THEN t.actual_fare ELSE 0 END)
                                                            AS total_working_revenue,
        ROUND(AVG(t.waiting_time_minute), 1)                AS avg_waiting_time_minute,
        ROUND(AVG(CASE WHEN t.is_completed THEN t.trip_duration_minute END), 1)
                                                            AS avg_trip_duration_minute
    FROM trips t
    GROUP BY t.request_date_key, t.driver_key, t.city
),

rating_agg AS (
    SELECT
        rating_date_key                                     AS date_key,
        driver_key,
        ROUND(AVG(rating_score), 2)                         AS avg_rating_score,
        COUNT(*)                                            AS total_rating_count
    FROM ratings
    GROUP BY rating_date_key, driver_key
),

payout_agg AS (
    SELECT
        payout_date_key                                     AS date_key,
        driver_key,
        SUM(CASE WHEN is_paid THEN final_payout_amount ELSE 0 END)
                                                            AS total_payout_amount
    FROM payouts
    GROUP BY payout_date_key, driver_key
)

SELECT
    ta.date_key,
    d.driver_id,
    d.full_name                                             AS driver_name,
    ta.city,

    -- Trip metrics
    ta.total_trip_assigned,
    ta.total_trip_completed,
    ta.total_trip_cancelled,
    ta.completion_rate,
    ta.total_actual_distance_km,
    ta.avg_waiting_time_minute,
    ta.avg_trip_duration_minute,

    -- Revenue
    CAST(ta.total_working_revenue AS NUMERIC)               AS total_working_revenue,
    CAST(COALESCE(pa.total_payout_amount, 0) AS NUMERIC)    AS total_payout_amount,

    -- Rating
    COALESCE(ra.avg_rating_score, 0)                        AS avg_rating_score,
    COALESCE(ra.total_rating_count, 0)                      AS total_rating_count,

    -- Productivity index: trip per jam aktif (estimasi)
    {{ safe_div(
        'CAST(ta.total_trip_completed AS FLOAT64)',
        'GREATEST(ta.total_actual_distance_km / 30.0, 1)'
    ) }}                                                    AS trips_per_hour_estimate

FROM trip_agg ta
LEFT JOIN drivers    d  ON ta.driver_key = d.driver_key
LEFT JOIN rating_agg ra ON ta.date_key   = ra.date_key  AND ta.driver_key = ra.driver_key
LEFT JOIN payout_agg pa ON ta.date_key   = pa.date_key  AND ta.driver_key = pa.driver_key
