-- ============================================================
-- gold_driver.dm_driver_monthly_summary
-- Data mart: ringkasan performa driver per bulan
-- Grain: 1 baris = 1 bulan (YYYY-MM) × 1 driver
-- Materialization: incremental merge (partisi month_key string)
-- ============================================================

{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = ['month_key', 'driver_id'],
        partition_by         = {
            'field'    : 'month_key',
            'data_type': 'string'
        },
        cluster_by           = ['city']
    )
}}

WITH trips AS (
    SELECT
        FORMAT_DATE('%Y-%m', DATE(t.request_ts, 'Asia/Jakarta'))  AS month_key,
        t.driver_key,
        t.city,
        t.is_completed,
        t.is_cancelled,
        t.actual_distance_km,
        t.actual_fare,
        t.request_ts
    FROM {{ ref('fct_trip') }} t

    {% if is_incremental() %}
    WHERE FORMAT_DATE('%Y-%m', DATE(t.request_ts, 'Asia/Jakarta')) >=
        FORMAT_DATE('%Y-%m',
            DATE_SUB(
                PARSE_DATE('%Y-%m', (SELECT MAX(month_key) FROM {{ this }})),
                INTERVAL 1 MONTH
            )
        )
    {% endif %}
),

ratings AS (
    SELECT
        FORMAT_DATE('%Y-%m', DATE(r.rated_ts, 'Asia/Jakarta'))  AS month_key,
        r.driver_key,
        r.rating_score
    FROM {{ ref('fct_rating') }} r
),

payouts AS (
    SELECT
        FORMAT_DATE('%Y-%m', dp.payout_date)    AS month_key,
        dp.driver_key,
        dp.final_payout_amount,
        dp.bonus_amount,
        dp.incentive_amount,
        dp.is_paid
    FROM {{ ref('fct_driver_payout') }} dp
),

drivers AS (
    SELECT driver_key, driver_id, full_name, city
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
),

trip_agg AS (
    SELECT
        month_key,
        driver_key,
        city,
        COUNT(*)                                                AS total_trip_assigned,
        COUNTIF(is_completed)                                   AS total_trip_completed,
        COUNTIF(is_cancelled)                                   AS total_trip_cancelled,
        {{ safe_div('COUNTIF(is_completed)', 'COUNT(*)') }}     AS completion_rate,
        ROUND(SUM(CASE WHEN is_completed THEN actual_distance_km ELSE 0 END), 2)
                                                                AS total_distance_km,
        CAST(SUM(CASE WHEN is_completed THEN actual_fare ELSE 0 END) AS NUMERIC)
                                                                AS total_trip_revenue,
        MIN(DATE(request_ts, 'Asia/Jakarta'))                   AS first_trip_date,
        MAX(DATE(request_ts, 'Asia/Jakarta'))                   AS last_trip_date,
        COUNT(DISTINCT DATE(request_ts, 'Asia/Jakarta'))        AS active_days
    FROM trips
    GROUP BY month_key, driver_key, city
),

rating_agg AS (
    SELECT
        month_key,
        driver_key,
        ROUND(AVG(rating_score), 2)                             AS avg_rating_score,
        COUNT(*)                                                AS total_ratings
    FROM ratings
    GROUP BY month_key, driver_key
),

payout_agg AS (
    SELECT
        month_key,
        driver_key,
        CAST(SUM(CASE WHEN is_paid THEN final_payout_amount ELSE 0 END) AS NUMERIC)
                                                                AS total_payout_amount,
        CAST(SUM(CASE WHEN is_paid THEN bonus_amount        ELSE 0 END) AS NUMERIC)
                                                                AS total_bonus_amount,
        CAST(SUM(CASE WHEN is_paid THEN incentive_amount    ELSE 0 END) AS NUMERIC)
                                                                AS total_incentive_amount
    FROM payouts
    GROUP BY month_key, driver_key
)

SELECT
    ta.month_key,
    d.driver_id,
    d.full_name                                                 AS driver_name,
    ta.city,

    -- Trip
    ta.total_trip_assigned,
    ta.total_trip_completed,
    ta.total_trip_cancelled,
    ta.completion_rate,
    ta.total_distance_km,
    ta.active_days,
    ta.first_trip_date,
    ta.last_trip_date,

    -- Rating
    COALESCE(ra.avg_rating_score, 0.0)                         AS avg_rating_score,
    COALESCE(ra.total_ratings, 0)                              AS total_ratings,

    -- Financial
    ta.total_trip_revenue,
    COALESCE(pa.total_payout_amount,   0)                      AS total_payout_amount,
    COALESCE(pa.total_bonus_amount,    0)                      AS total_bonus_amount,
    COALESCE(pa.total_incentive_amount,0)                      AS total_incentive_amount,

    -- Derived: avg trip per hari aktif
    {{ safe_div('CAST(ta.total_trip_completed AS FLOAT64)', 'ta.active_days') }}
                                                               AS avg_trips_per_active_day

FROM trip_agg ta
LEFT JOIN drivers    d  ON ta.driver_key = d.driver_key
LEFT JOIN rating_agg ra ON ta.month_key  = ra.month_key AND ta.driver_key = ra.driver_key
LEFT JOIN payout_agg pa ON ta.month_key  = pa.month_key AND ta.driver_key = pa.driver_key
