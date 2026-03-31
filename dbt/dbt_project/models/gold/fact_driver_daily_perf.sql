{{
  config(
    materialized        = 'incremental',
    unique_key          = ['driver_id', 'perf_date'],
    incremental_strategy= 'merge',
    partition_by        = {'field': 'perf_date', 'data_type': 'date'},
    cluster_by          = ['driver_tier', 'vehicle_type_code'],
    tags                = ['gold', 'fact']
  )
}}

/*
  GOLD FACT: fact_driver_daily_perf
  ══════════════════════════════════════════════════════════════
  Grain: driver_id × date — satu baris per pengemudi per hari.
  Digunakan untuk: driver scorecard, eligibilitas insentif, supply analytics.

  Sources: slv_rides (performa), slv_payments → driver_incentives (bonus)
*/

WITH rides AS (
    SELECT
        ride_date            AS perf_date,
        driver_id,
        driver_tier,
        vehicle_type_code,
        pickup_city          AS operating_city,

        COUNT(*)                                        AS total_requests,
        SUM(CAST(is_completed AS INT64))                AS completed_rides,
        SUM(CAST(is_cancelled AS INT64))                AS cancelled_rides,
        SUM(CAST(is_surge_ride AS INT64))               AS surge_rides,
        SUM(distance_km)                                AS total_distance_km,
        SUM(duration_min_actual)                        AS total_online_minutes,
        SUM(gmv)                                        AS total_gmv,
        SUM(driver_earning)                             AS total_driver_earning,
        AVG(driver_rating)                              AS avg_passenger_rating,
        AVG(wait_time_min)                              AS avg_wait_time_min,
        MAX(updated_at)                                 AS updated_at

    FROM {{ ref('slv_rides') }}

    {% if is_incremental() %}
    WHERE updated_at > (
        SELECT TIMESTAMP_SUB(MAX(updated_at), INTERVAL 1 HOUR) FROM {{ this }}
    )
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
),

incentives AS (
    SELECT
        period_date,
        driver_id,
        SUM(amount)                                     AS total_incentive_amount,
        SUM(CASE WHEN incentive_type='BONUS_TRIP' THEN amount ELSE 0 END)
                                                        AS bonus_trip_amount,
        SUM(CASE WHEN incentive_type='PEAK_HOUR'  THEN amount ELSE 0 END)
                                                        AS peak_hour_amount,
        SUM(CASE WHEN incentive_type='SURGE'      THEN amount ELSE 0 END)
                                                        AS surge_bonus_amount,
        COUNT(*)                                        AS incentive_count
    FROM {{ source('mysql_raw', 'driver_incentives') }}
    GROUP BY 1, 2
),

dim_driver AS (
    SELECT driver_id, driver_sk
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
),

dim_date AS (
    SELECT full_date, date_key
    FROM {{ ref('dim_date') }}
)

SELECT
    -- ── Keys ──────────────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['r.driver_id', 'r.perf_date']) }}
                                                        AS driver_daily_sk,
    r.driver_id,
    COALESCE(dr.driver_sk, 'UNKNOWN')                   AS driver_sk,
    dd.date_key                                         AS perf_date_key,

    -- ── Attributes (degenerate dims) ──────────────────────────────
    r.perf_date,
    r.driver_tier,
    r.vehicle_type_code,
    r.operating_city,

    -- ── Volume measures ───────────────────────────────────────────
    r.total_requests,
    r.completed_rides,
    r.cancelled_rides,
    r.surge_rides,

    -- ── Rate KPIs ─────────────────────────────────────────────────
    ROUND(SAFE_DIVIDE(r.completed_rides, NULLIF(r.total_requests, 0)) * 100, 2)
                                                        AS completion_rate_pct,
    ROUND(SAFE_DIVIDE(r.cancelled_rides, NULLIF(r.total_requests, 0)) * 100, 2)
                                                        AS cancellation_rate_pct,

    -- ── Revenue measures ──────────────────────────────────────────
    r.total_gmv,
    r.total_driver_earning,
    COALESCE(i.total_incentive_amount, 0)               AS total_incentive_amount,
    COALESCE(i.bonus_trip_amount, 0)                    AS bonus_trip_amount,
    COALESCE(i.peak_hour_amount, 0)                     AS peak_hour_amount,
    COALESCE(i.surge_bonus_amount, 0)                   AS surge_bonus_amount,
    r.total_driver_earning
      + COALESCE(i.total_incentive_amount, 0)           AS total_income,

    -- ── Service measures ──────────────────────────────────────────
    r.total_distance_km,
    r.total_online_minutes,
    r.avg_passenger_rating,
    r.avg_wait_time_min,
    ROUND(SAFE_DIVIDE(r.total_distance_km,
          NULLIF(r.total_online_minutes / 60.0, 0)), 1) AS avg_speed_kmh,

    r.updated_at

FROM rides r
LEFT JOIN incentives   i  ON r.driver_id  = i.driver_id
                          AND r.perf_date = i.period_date
LEFT JOIN dim_driver   dr ON r.driver_id  = dr.driver_id
LEFT JOIN dim_date     dd ON r.perf_date  = dd.full_date
