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
    cluster_by          = ['ride_status', 'vehicle_type_id'],
    tags                = ['silver', 'staging', 'rides']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  stg_rides
--  Purpose : Enrich & validate ride records from Bronze.
--  Incremental: merge on ride_id, look back N days on updated_at
--  to capture late-arriving status updates (e.g. ACCEPTED→COMPLETED).
-- ══════════════════════════════════════════════════════════════

with source as (

    select * from {{ source('bronze_pg', 'rides') }}

    {% if is_incremental() %}
    where updated_at >= timestamp_sub(
        current_timestamp(),
        interval {{ var('incremental_lookback_days') }} day
    )
    {% endif %}

),

enriched as (

    select
        -- ── Identity ──────────────────────────────────────────────
        ride_id,
        ride_code,
        driver_id,
        passenger_id,
        vehicle_type_id,

        -- ── Zones ─────────────────────────────────────────────────
        pickup_zone_id,
        dropoff_zone_id,
        pickup_address,
        dropoff_address,
        pickup_lat,
        pickup_lon,
        dropoff_lat,
        dropoff_lon,

        -- ── Timestamps ────────────────────────────────────────────
        requested_at,
        date(requested_at)                                          as requested_date,
        accepted_at,
        picked_up_at,
        completed_at,
        cancelled_at,

        -- ── Derived timing durations ──────────────────────────────
        case
            when accepted_at is not null
            then timestamp_diff(accepted_at, requested_at, second)
        end                                                         as acceptance_duration_secs,

        case
            when picked_up_at is not null and accepted_at is not null
            then timestamp_diff(picked_up_at, accepted_at, second)
        end                                                         as pickup_wait_secs,

        case
            when completed_at is not null and picked_up_at is not null
            then timestamp_diff(completed_at, picked_up_at, second)
        end                                                         as trip_duration_secs,

        -- ── Metrics ───────────────────────────────────────────────
        coalesce(distance_km, 0)                                    as distance_km,
        coalesce(duration_minutes, 0)                               as duration_minutes,

        -- ── Fare components ───────────────────────────────────────
        coalesce(base_fare, 0)                                      as base_fare,
        coalesce(distance_fare, 0)                                  as distance_fare,
        coalesce(time_fare, 0)                                      as time_fare,
        coalesce(surge_multiplier, 1.0)                             as surge_multiplier,
        coalesce(promo_discount, 0)                                 as promo_discount,
        coalesce(total_fare, 0)                                     as total_fare,

        -- ── Derived fare flags ────────────────────────────────────
        case when coalesce(surge_multiplier, 1.0) > 1.0 then true else false end
                                                                    as is_surge,
        case when coalesce(promo_discount, 0) > 0        then true else false end
                                                                    as has_promo,
        case
            when extract(hour from requested_at) between 7  and 9  then 'morning_peak'
            when extract(hour from requested_at) between 17 and 19 then 'evening_peak'
            when extract(hour from requested_at) between 22 and 23
              or extract(hour from requested_at) between 0  and 4  then 'late_night'
            else 'off_peak'
        end                                                         as time_bucket,

        -- ── Status ────────────────────────────────────────────────
        upper(ride_status)                                          as ride_status,
        cancellation_reason,

        -- ── Ratings ───────────────────────────────────────────────
        passenger_rating,
        driver_rating,
        notes,

        -- ── Timestamps for lineage ────────────────────────────────
        created_at,
        updated_at,
        _source_system

    from source

)

select * from enriched
