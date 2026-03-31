{{
  config(
    materialized = 'table',
    tags         = ['gold', 'mart', 'executive']
  )
}}

/*
  GOLD MART: mart_executive_summary
  ══════════════════════════════════════════════════════════════
  Grain: one row per calendar date.

  The single source of truth for C-level and board dashboards.
  Includes:
    - Daily demand & revenue metrics
    - Payment channel breakdown
    - 7-day rolling averages
    - Day-over-day change percentages
    - All-time cumulative metrics

  Refresh: full table rebuild (small, < 1000 rows/3 years)
*/

WITH daily_rides AS (
    SELECT
        ride_date,
        ride_date_key,

        -- Volume
        SUM(ride_count)                                 AS total_requests,
        SUM(completed_count)                            AS total_completed,
        SUM(cancelled_count)                            AS total_cancelled,
        SUM(surge_ride_count)                           AS total_surge_rides,

        -- Revenue
        SUM(gmv)                                        AS gross_merchandise_value,
        SUM(platform_fee)                               AS platform_revenue,
        SUM(driver_earning)                             AS driver_payouts,
        SUM(promo_discount)                             AS total_promo_spend,

        -- Averages
        AVG(total_fare)                                 AS avg_fare_per_ride,
        AVG(distance_km)                                AS avg_distance_km,
        AVG(wait_time_min)                              AS avg_wait_min,
        AVG(driver_rating)                              AS avg_driver_rating,
        AVG(passenger_rating)                           AS avg_passenger_rating,
        AVG(processing_lag_seconds)                     AS avg_payment_lag_sec,

        -- Unique active users
        COUNT(DISTINCT driver_id)                       AS active_drivers,
        COUNT(DISTINCT passenger_id)                    AS active_passengers,

        -- Conversion rates
        ROUND(SAFE_DIVIDE(SUM(completed_count),
            NULLIF(SUM(ride_count),0)) * 100, 2)        AS completion_rate_pct,
        ROUND(SAFE_DIVIDE(SUM(cancelled_count),
            NULLIF(SUM(ride_count),0)) * 100, 2)        AS cancellation_rate_pct,
        ROUND(SAFE_DIVIDE(SUM(surge_ride_count),
            NULLIF(SUM(ride_count),0)) * 100, 2)        AS surge_rate_pct,

        -- Payment mix
        SUM(CASE WHEN is_ewallet THEN gmv ELSE 0 END)  AS ewallet_gmv,
        SUM(CASE WHEN is_cash    THEN gmv ELSE 0 END)  AS cash_gmv,
        SUM(CASE WHEN is_card    THEN gmv ELSE 0 END)  AS card_gmv,

        -- Vehicle mix
        SUM(CASE WHEN vehicle_type_code='MOTOR'   THEN ride_count ELSE 0 END) AS motor_rides,
        SUM(CASE WHEN vehicle_type_code='ECONOMI' THEN ride_count ELSE 0 END) AS economi_rides,
        SUM(CASE WHEN vehicle_type_code='COMFORT' THEN ride_count ELSE 0 END) AS comfort_rides,
        SUM(CASE WHEN vehicle_type_code='SUV'     THEN ride_count ELSE 0 END) AS suv_rides,
        SUM(CASE WHEN vehicle_type_code='PREMIUM' THEN ride_count ELSE 0 END) AS premium_rides,

        -- Time-of-day demand
        SUM(CASE WHEN time_of_day_bucket='MORNING_PEAK'  THEN ride_count ELSE 0 END) AS morning_peak_rides,
        SUM(CASE WHEN time_of_day_bucket='EVENING_PEAK'  THEN ride_count ELSE 0 END) AS evening_peak_rides,
        SUM(CASE WHEN day_type='WEEKEND'                 THEN ride_count ELSE 0 END) AS weekend_rides

    FROM {{ ref('fact_rides') }}
    GROUP BY ride_date, ride_date_key
),

daily_refunds AS (
    SELECT
        payment_date                                    AS refund_date,
        SUM(refund_count)                               AS total_refunds,
        SUM(CASE WHEN is_refunded THEN amount ELSE 0 END) AS total_refund_amount
    FROM {{ ref('fact_payments') }}
    GROUP BY payment_date
),

dim_date AS (
    SELECT full_date, date_key, is_weekend, is_public_holiday,
           holiday_name, day_name, year, month_number, month_name,
           quarter_label, week_of_year
    FROM {{ ref('dim_date') }}
)

SELECT
    -- ── Calendar ─────────────────────────────────────────────────
    r.ride_date,
    r.ride_date_key,
    dd.is_weekend,
    dd.is_public_holiday,
    dd.holiday_name,
    dd.day_name,
    dd.year,
    dd.month_number,
    dd.month_name,
    dd.quarter_label,
    dd.week_of_year,

    -- ── Demand ───────────────────────────────────────────────────
    r.total_requests,
    r.total_completed,
    r.total_cancelled,
    r.total_surge_rides,
    r.active_drivers,
    r.active_passengers,

    -- ── Revenue ───────────────────────────────────────────────────
    r.gross_merchandise_value                               AS gmv,
    r.platform_revenue,
    r.driver_payouts,
    r.total_promo_spend,
    COALESCE(ref.total_refunds, 0)                          AS total_refunds,
    COALESCE(ref.total_refund_amount, 0)                    AS refund_amount,

    -- ── Rate KPIs ─────────────────────────────────────────────────
    r.completion_rate_pct,
    r.cancellation_rate_pct,
    r.surge_rate_pct,
    r.avg_fare_per_ride,
    r.avg_distance_km,
    r.avg_wait_min,
    r.avg_driver_rating,
    r.avg_passenger_rating,
    r.avg_payment_lag_sec,

    -- ── Payment channel mix ───────────────────────────────────────
    r.ewallet_gmv,
    r.cash_gmv,
    r.card_gmv,
    ROUND(SAFE_DIVIDE(r.ewallet_gmv, NULLIF(r.gross_merchandise_value,0))*100,2)
                                                            AS ewallet_gmv_pct,

    -- ── Vehicle mix ───────────────────────────────────────────────
    r.motor_rides,
    r.economi_rides,
    r.comfort_rides,
    r.suv_rides,
    r.premium_rides,

    -- ── Time-of-day ───────────────────────────────────────────────
    r.morning_peak_rides,
    r.evening_peak_rides,
    r.weekend_rides,

    -- ── Rolling windows ───────────────────────────────────────────
    ROUND(AVG(r.gross_merchandise_value) OVER (
        ORDER BY r.ride_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                   AS gmv_7d_rolling_avg,

    ROUND(AVG(r.total_completed) OVER (
        ORDER BY r.ride_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0)                                                   AS rides_7d_rolling_avg,

    ROUND(AVG(r.completion_rate_pct) OVER (
        ORDER BY r.ride_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                   AS completion_rate_7d_avg,

    ROUND(AVG(r.active_drivers) OVER (
        ORDER BY r.ride_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 0)                                                   AS active_drivers_30d_avg,

    -- ── Day-over-day ─────────────────────────────────────────────
    LAG(r.gross_merchandise_value) OVER (ORDER BY r.ride_date)
                                                            AS prev_day_gmv,
    ROUND(SAFE_DIVIDE(
        r.gross_merchandise_value
          - LAG(r.gross_merchandise_value) OVER (ORDER BY r.ride_date),
        NULLIF(LAG(r.gross_merchandise_value) OVER (ORDER BY r.ride_date), 0)
    ) * 100, 2)                                             AS gmv_dod_pct,

    -- Same day last week
    LAG(r.gross_merchandise_value, 7) OVER (ORDER BY r.ride_date)
                                                            AS same_day_last_week_gmv,
    ROUND(SAFE_DIVIDE(
        r.gross_merchandise_value
          - LAG(r.gross_merchandise_value, 7) OVER (ORDER BY r.ride_date),
        NULLIF(LAG(r.gross_merchandise_value, 7) OVER (ORDER BY r.ride_date), 0)
    ) * 100, 2)                                             AS gmv_wow_pct,

    -- ── Cumulative (all-time running totals) ──────────────────────
    SUM(r.gross_merchandise_value) OVER (ORDER BY r.ride_date)
                                                            AS cumulative_gmv,
    SUM(r.total_completed) OVER (ORDER BY r.ride_date)
                                                            AS cumulative_rides

FROM daily_rides r
LEFT JOIN daily_refunds ref ON r.ride_date   = ref.refund_date
LEFT JOIN dim_date      dd  ON r.ride_date   = dd.full_date
ORDER BY r.ride_date
