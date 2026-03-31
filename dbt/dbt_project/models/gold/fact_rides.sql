{{
  config(
    materialized        = 'incremental',
    unique_key          = 'ride_id',
    incremental_strategy= 'merge',
    partition_by        = {'field': 'ride_date', 'data_type': 'date'},
    cluster_by          = ['vehicle_type_code', 'pickup_city', 'ride_status'],
    tags                = ['gold', 'fact']
  )
}}

/*
  GOLD FACT: fact_rides
  ══════════════════════════════════════════════════════════════
  Central fact table — one row per ride.
  Contains all measures and foreign keys to dims.

  Grain: one ride per row
  Measures: fares, distances, durations, ratings, counts
  Dims: driver, passenger, pickup_zone, dropoff_zone,
        vehicle_type, payment_method, date
*/

WITH rides AS (
    SELECT * FROM {{ ref('slv_rides') }}
    {% if is_incremental() %}
    WHERE updated_at > (
        SELECT TIMESTAMP_SUB(MAX(updated_at), INTERVAL 1 HOUR) FROM {{ this }}
    )
    {% endif %}
),

dim_driver AS (
    SELECT driver_id, driver_sk
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
),

dim_passenger AS (
    SELECT passenger_id, passenger_sk
    FROM {{ ref('dim_passenger') }}
    WHERE is_current = TRUE
),

dim_zone AS (
    SELECT zone_id, zone_sk
    FROM {{ ref('dim_zone') }}
),

dim_vehicle_type AS (
    SELECT vehicle_type_id, vehicle_type_sk
    FROM {{ ref('dim_vehicle_type') }}
),

dim_payment_method AS (
    SELECT method_id, payment_method_sk
    FROM {{ ref('dim_payment_method') }}
),

dim_date AS (
    SELECT full_date, date_key
    FROM {{ ref('dim_date') }}
)

SELECT
    -- ── Surrogate key ────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['r.ride_id']) }}
                                                    AS ride_sk,

    -- ── Natural key ─────────────────────────────────────────────
    r.ride_id,
    r.ride_code,

    -- ── Foreign keys (dim lookups) ───────────────────────────────
    dd.date_key                                     AS ride_date_key,
    COALESCE(dr.driver_sk,    'UNKNOWN')            AS driver_sk,
    COALESCE(px.passenger_sk, 'UNKNOWN')            AS passenger_sk,
    COALESCE(pz.zone_sk,      'UNKNOWN')            AS pickup_zone_sk,
    COALESCE(dz.zone_sk,      'UNKNOWN')            AS dropoff_zone_sk,
    COALESCE(vt.vehicle_type_sk, 'UNKNOWN')         AS vehicle_type_sk,
    COALESCE(pm.payment_method_sk, 'UNKNOWN')       AS payment_method_sk,

    -- ── Degenerate dimensions (kept on fact) ────────────────────
    r.ride_status,
    r.vehicle_type_code,
    r.pickup_city,
    r.dropoff_city,
    r.time_of_day_bucket,
    r.day_type,
    r.request_hour,
    r.revenue_bucket,
    r.driver_tier,
    r.passenger_loyalty_tier,
    r.passenger_age_bucket,
    r.passenger_gender,
    r.payment_status,
    r.payment_gateway,
    r.device_type,
    r.pickup_street_type,
    r.dropoff_street_type,

    -- ── Boolean flags ───────────────────────────────────────────
    r.is_completed,
    r.is_cancelled,
    r.is_surge_ride,
    r.is_same_zone,
    r.passenger_is_verified,
    r.is_ewallet,
    r.is_cash,

    -- ── Fare measures ────────────────────────────────────────────
    r.base_fare,
    r.distance_fare,
    r.time_fare,
    r.surge_multiplier,
    r.promo_discount,
    r.total_fare,
    COALESCE(r.platform_fee,   r.total_fare * 0.20)  AS platform_fee,
    COALESCE(r.driver_earning, r.total_fare * 0.80)  AS driver_earning,

    -- GMV contribution (zero for cancelled)
    CASE WHEN r.is_completed THEN r.total_fare ELSE 0 END
                                                     AS gmv,

    -- ── Distance & time measures ─────────────────────────────────
    r.distance_km,
    r.duration_min_actual,
    r.wait_time_min,
    r.pickup_wait_min,
    r.avg_speed_kmh,

    -- ── Rating measures ─────────────────────────────────────────
    r.passenger_rating,
    r.driver_rating,

    -- ── Payment measures ─────────────────────────────────────────
    r.payment_amount,
    r.processing_lag_seconds,

    -- ── Additive count measures (for aggregation) ────────────────
    1                                                AS ride_count,
    CAST(r.is_completed AS INT64)                    AS completed_count,
    CAST(r.is_cancelled AS INT64)                    AS cancelled_count,
    CAST(r.is_surge_ride AS INT64)                   AS surge_ride_count,

    -- ── Cancellation ─────────────────────────────────────────────
    r.cancellation_reason,

    -- ── Timestamps ───────────────────────────────────────────────
    r.ride_date,
    r.requested_at,
    r.accepted_at,
    r.picked_up_at,
    r.completed_at,
    r.cancelled_at,
    r.paid_at,
    r.updated_at

FROM rides r

-- Dim lookups
LEFT JOIN dim_driver         dr ON r.driver_id         = dr.driver_id
LEFT JOIN dim_passenger      px ON r.passenger_id      = px.passenger_id
LEFT JOIN dim_zone           pz ON r.pickup_zone_id    = pz.zone_id
LEFT JOIN dim_zone           dz ON r.dropoff_zone_id   = dz.zone_id
LEFT JOIN dim_vehicle_type   vt ON r.vehicle_type_id   = vt.vehicle_type_id
LEFT JOIN dim_payment_method pm ON r.payment_method_id = pm.method_id
LEFT JOIN dim_date           dd ON r.ride_date         = dd.full_date
