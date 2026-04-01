{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'merge',
    unique_key          = 'ride_id',
    partition_by        = {
      'field': 'requested_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by          = ['ride_status', 'vehicle_type_id', 'pickup_zone_id'],
    tags                = ['silver', 'fact', 'rides']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  fct_rides
--  Purpose : Central fact table — one row per ride.
--  Grain   : ride_id
--  Joins   : dim_drivers, dim_passengers, dim_zones (x2), dim_vehicle_types, dim_date
--  Incremental: merge strategy, lookback {{ var('incremental_lookback_days') }} days
-- ══════════════════════════════════════════════════════════════

with rides as (

    select * from {{ ref('stg_rides') }}

    {% if is_incremental() %}
    where updated_at >= timestamp_sub(
        current_timestamp(),
        interval {{ var('incremental_lookback_days') }} day
    )
    {% endif %}

),

drivers as (
    select driver_id, driver_name, vehicle_type_code, vehicle_type_name,
           experience_tier, home_zone_code, driver_status
    from {{ ref('dim_drivers') }}
),

passengers as (
    select passenger_id, passenger_name, loyalty_tier, age_bucket,
           gender, home_zone_code as pax_home_zone_code
    from {{ ref('dim_passengers') }}
),

pickup_zones as (
    select zone_id, zone_code as pickup_zone_code, zone_name as pickup_zone_name,
           city as pickup_city, province as pickup_province, area_type as pickup_area_type
    from {{ ref('dim_zones') }}
),

dropoff_zones as (
    select zone_id, zone_code as dropoff_zone_code, zone_name as dropoff_zone_name,
           city as dropoff_city, province as dropoff_province, area_type as dropoff_area_type
    from {{ ref('dim_zones') }}
),

vehicle_types as (
    select vehicle_type_id, type_code, type_name, vehicle_category, fare_tier, capacity
    from {{ ref('dim_vehicle_types') }}
),

dates as (
    select date_id, day_name, month_num, month_name, year_num,
           quarter_num, iso_year_week, is_weekend, time_bucket
    from {{ ref('dim_date') }}
    -- dim_date doesn't have time_bucket; time_bucket comes from stg_rides
    -- kept simple here
),

final as (

    select
        -- ── Fact keys ─────────────────────────────────────────────
        r.ride_id,
        r.ride_code,

        -- ── FK to dimensions ──────────────────────────────────────
        r.driver_id,
        r.passenger_id,
        r.vehicle_type_id,
        r.pickup_zone_id,
        r.dropoff_zone_id,
        r.requested_date                                    as date_id,

        -- ── Status & classification ───────────────────────────────
        r.ride_status,
        r.time_bucket,
        r.is_surge,
        r.has_promo,

        -- ── Driver attributes (denormalised for query performance) ─
        d.driver_name,
        d.vehicle_type_code,
        d.vehicle_type_name,
        d.experience_tier                                   as driver_tier,

        -- ── Passenger attributes ──────────────────────────────────
        p.passenger_name,
        p.loyalty_tier                                      as passenger_tier,
        p.age_bucket                                        as passenger_age_bucket,
        p.gender                                            as passenger_gender,

        -- ── Zone attributes ───────────────────────────────────────
        pz.pickup_zone_code,
        pz.pickup_zone_name,
        pz.pickup_city,
        pz.pickup_province,
        pz.pickup_area_type,

        dz.dropoff_zone_code,
        dz.dropoff_zone_name,
        dz.dropoff_city,
        dz.dropoff_province,

        -- ── Same-zone flag ────────────────────────────────────────
        case when r.pickup_zone_id = r.dropoff_zone_id then true else false end
                                                            as is_same_zone_trip,

        -- ── Vehicle attributes ────────────────────────────────────
        vt.vehicle_category,
        vt.fare_tier,
        vt.capacity                                         as vehicle_capacity,

        -- ── Measures: timing ──────────────────────────────────────
        r.requested_at,
        r.accepted_at,
        r.picked_up_at,
        r.completed_at,
        r.cancelled_at,
        r.acceptance_duration_secs,
        r.pickup_wait_secs,
        r.trip_duration_secs,
        r.distance_km,
        r.duration_minutes,

        -- ── Measures: fare ────────────────────────────────────────
        r.base_fare,
        r.distance_fare,
        r.time_fare,
        r.surge_multiplier,
        r.promo_discount,
        r.total_fare,

        -- ── Measures: ratings ─────────────────────────────────────
        r.passenger_rating,
        r.driver_rating,

        -- ── Cancellation ──────────────────────────────────────────
        r.cancellation_reason,

        -- ── Metadata ──────────────────────────────────────────────
        r.updated_at,
        r._source_system

    from rides r
    left join drivers d         on r.driver_id = d.driver_id
    left join passengers p      on r.passenger_id = p.passenger_id
    left join pickup_zones pz   on r.pickup_zone_id = pz.zone_id
    left join dropoff_zones dz  on r.dropoff_zone_id = dz.zone_id
    left join vehicle_types vt  on r.vehicle_type_id = vt.vehicle_type_id

)

select * from final
