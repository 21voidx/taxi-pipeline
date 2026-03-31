{{
  config(
    materialized        = 'incremental',
    unique_key          = 'ride_id',
    incremental_strategy= 'merge',
    partition_by        = {
      'field': 'ride_date',
      'data_type': 'date'
    },
    cluster_by          = ['ride_status', 'vehicle_type_code', 'pickup_city'],
    tags                = ['silver', 'core']
  )
}}

/*
  SILVER: slv_rides
  ══════════════════════════════════════════════════════════════
  The central model — joins rides with drivers, passengers, zones,
  vehicle types, payments.

  ✓ JOIN    – drivers + passengers + zones (pickup & dropoff) + vt
  ✓ CLEAN   – NULL coalescing, status normalisation
  ✓ DERIVE  – wait_time_min, trip_speed_kmh, is_surge,
               time_of_day_bucket, day_type, revenue_bucket
  ✓ REGEX   – extract street type from address
*/

WITH rides AS (
    SELECT * FROM {{ ref('brz_rides') }}
),
drivers AS (
    SELECT
        driver_id,
        full_name            AS driver_name,
        driver_tier,
        vehicle_type_code,
        vehicle_type_name,
        vehicle_brand,
        vehicle_model,
        license_plate_normalised AS license_plate,
        rating               AS driver_rating_avg,
        home_zone_code,
        home_city            AS driver_city
    FROM {{ ref('slv_drivers') }}
),
passengers AS (
    SELECT
        passenger_id,
        full_name            AS passenger_name,
        loyalty_tier,
        age_bucket,
        gender,
        home_zone_code       AS passenger_zone_code,
        home_city            AS passenger_city,
        is_verified          AS passenger_is_verified
    FROM {{ ref('slv_passengers') }}
),
zones AS (
    SELECT zone_id, zone_code, zone_name, city, latitude, longitude
    FROM {{ ref('brz_zones') }}
),
payments AS (
    SELECT
        ride_id,
        payment_id,
        payment_status,
        payment_gateway,
        method_id,
        amount               AS payment_amount,
        platform_fee,
        driver_earning,
        promo_discount       AS payment_promo_discount,
        paid_at
    FROM {{ ref('slv_payments') }}
    WHERE payment_status = 'SUCCESS'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY paid_at DESC) = 1
)

SELECT
    -- ── Keys ────────────────────────────────────────────────────
    r.ride_id,
    r.ride_code,

    -- ── Dates & time ────────────────────────────────────────────
    DATE(r.requested_at)                              AS ride_date,
    r.requested_at,
    r.accepted_at,
    r.picked_up_at,
    r.completed_at,
    r.cancelled_at,

    -- wait time = minutes from requested to accepted
    TIMESTAMP_DIFF(r.accepted_at, r.requested_at, MINUTE)
                                                      AS wait_time_min,

    -- pickup wait = minutes from accepted to picked up
    TIMESTAMP_DIFF(r.picked_up_at, r.accepted_at, MINUTE)
                                                      AS pickup_wait_min,

    -- actual duration
    COALESCE(r.duration_minutes,
      TIMESTAMP_DIFF(r.completed_at, r.picked_up_at, MINUTE))
                                                      AS duration_min_actual,

    -- time of day bucket
    CASE
      WHEN EXTRACT(HOUR FROM r.requested_at) BETWEEN 5  AND 9  THEN 'MORNING_PEAK'
      WHEN EXTRACT(HOUR FROM r.requested_at) BETWEEN 10 AND 11 THEN 'MID_MORNING'
      WHEN EXTRACT(HOUR FROM r.requested_at) BETWEEN 12 AND 13 THEN 'LUNCH'
      WHEN EXTRACT(HOUR FROM r.requested_at) BETWEEN 14 AND 16 THEN 'AFTERNOON'
      WHEN EXTRACT(HOUR FROM r.requested_at) BETWEEN 17 AND 20 THEN 'EVENING_PEAK'
      WHEN EXTRACT(HOUR FROM r.requested_at) BETWEEN 21 AND 23 THEN 'NIGHT'
      ELSE 'LATE_NIGHT'
    END                                               AS time_of_day_bucket,

    -- day type
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM r.requested_at) IN (1, 7) THEN 'WEEKEND'
      ELSE 'WEEKDAY'
    END                                               AS day_type,

    EXTRACT(HOUR FROM r.requested_at)                 AS request_hour,
    FORMAT_DATE('%A', DATE(r.requested_at))           AS day_name,

    -- ── Driver ──────────────────────────────────────────────────
    r.driver_id,
    d.driver_name,
    d.driver_tier,
    d.vehicle_type_code,
    d.vehicle_type_name,
    d.vehicle_brand,
    d.vehicle_model,
    d.license_plate,
    d.driver_rating_avg,
    d.home_zone_code                                  AS driver_home_zone,
    d.driver_city,

    -- ── Passenger ───────────────────────────────────────────────
    r.passenger_id,
    p.passenger_name,
    p.loyalty_tier                                    AS passenger_loyalty_tier,
    p.age_bucket                                      AS passenger_age_bucket,
    p.gender                                          AS passenger_gender,
    p.passenger_is_verified,

    -- ── Zones ───────────────────────────────────────────────────
    r.pickup_zone_id,
    pz.zone_code                                      AS pickup_zone_code,
    pz.zone_name                                      AS pickup_zone_name,
    pz.city                                           AS pickup_city,
    r.pickup_lat,
    r.pickup_lon,

    r.dropoff_zone_id,
    dz.zone_code                                      AS dropoff_zone_code,
    dz.zone_name                                      AS dropoff_zone_name,
    dz.city                                           AS dropoff_city,
    r.dropoff_lat,
    r.dropoff_lon,

    -- is intra-zone trip?
    (r.pickup_zone_id = r.dropoff_zone_id)            AS is_same_zone,

    -- ── Address parsing via REGEX ────────────────────────────────
    -- Extract street type (Jalan, Gang, Komplek, etc.)
    REGEXP_EXTRACT(
      UPPER(COALESCE(r.pickup_address,'')),
      r'^(JL|JALAN|JLN|GNG|GANG|KMP|KOMPLEK|BSD|BLOK|RUKO)\b'
    )                                                 AS pickup_street_type,

    REGEXP_EXTRACT(
      UPPER(COALESCE(r.dropoff_address,'')),
      r'^(JL|JALAN|JLN|GNG|GANG|KMP|KOMPLEK|BSD|BLOK|RUKO)\b'
    )                                                 AS dropoff_street_type,

    -- ── Trip metrics ────────────────────────────────────────────
    r.distance_km,
    ROUND(
      r.distance_km / NULLIF(r.duration_minutes / 60.0, 0), 1
    )                                                 AS avg_speed_kmh,

    -- ── Fare ────────────────────────────────────────────────────
    r.base_fare,
    r.distance_fare,
    r.time_fare,
    r.surge_multiplier,
    r.promo_discount,
    r.total_fare,

    (r.surge_multiplier > 1.0)                        AS is_surge_ride,

    -- revenue bucket
    CASE
      WHEN r.total_fare = 0       THEN 'FREE'
      WHEN r.total_fare < 15000   THEN 'LOW'
      WHEN r.total_fare < 50000   THEN 'MEDIUM'
      WHEN r.total_fare < 100000  THEN 'HIGH'
      ELSE 'PREMIUM'
    END                                               AS revenue_bucket,

    -- ── Status ──────────────────────────────────────────────────
    r.ride_status,
    (r.ride_status = 'COMPLETED')                     AS is_completed,
    (r.ride_status = 'CANCELLED')                     AS is_cancelled,
    r.cancellation_reason,

    -- ── Ratings ─────────────────────────────────────────────────
    r.passenger_rating,
    r.driver_rating,

    -- ── Payment (from MySQL) ─────────────────────────────────────
    pay.payment_id,
    pay.payment_status,
    pay.payment_gateway,
    pay.method_id                                     AS payment_method_id,
    pay.payment_amount,
    pay.platform_fee,
    pay.driver_earning,
    pay.paid_at,

    -- ── Metadata ────────────────────────────────────────────────
    r.created_at,
    r.updated_at,
    r._loaded_at,
    r._row_hash

FROM rides r
LEFT JOIN drivers    d  ON r.driver_id       = d.driver_id
LEFT JOIN passengers p  ON r.passenger_id    = p.passenger_id
LEFT JOIN zones      pz ON r.pickup_zone_id  = pz.zone_id
LEFT JOIN zones      dz ON r.dropoff_zone_id = dz.zone_id
LEFT JOIN payments   pay ON r.ride_id        = pay.ride_id

{% if is_incremental() %}
WHERE r.updated_at > (
    SELECT TIMESTAMP_SUB(MAX(updated_at), INTERVAL 1 HOUR) FROM {{ this }}
)
{% endif %}
