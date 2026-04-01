{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'merge',
    unique_key          = 'event_id',
    partition_by        = {
      'field': 'occurred_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by          = ['event_type'],
    tags                = ['silver', 'fact', 'events']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  fct_ride_events
--  Purpose : Event-level fact with ride context joined in.
--  Grain   : event_id (one row per ride lifecycle event)
--  Useful  : funnel analysis, acceptance rate, cancellation root cause
-- ══════════════════════════════════════════════════════════════

with events as (

    select * from {{ ref('stg_ride_events') }}

    {% if is_incremental() %}
    where occurred_at >= timestamp_sub(
        current_timestamp(),
        interval {{ var('incremental_lookback_days') }} day
    )
    {% endif %}

),

rides as (
    -- Only bring lightweight columns to avoid huge join
    select ride_id, ride_status, vehicle_type_id, pickup_zone_id,
           driver_id, passenger_id, total_fare, requested_date
    from {{ ref('fct_rides') }}
),

final as (

    select
        e.event_id,
        e.ride_id,
        e.event_type,
        e.occurred_at,
        e.occurred_date,

        -- ── Parsed payload fields ─────────────────────────────────
        e.req_surge,
        e.req_zone,
        e.acc_driver_id,
        e.cmp_fare,
        e.cmp_distance_km,
        e.can_reason,

        -- ── Ride context (denorm) ─────────────────────────────────
        r.ride_status,
        r.vehicle_type_id,
        r.pickup_zone_id,
        r.driver_id,
        r.passenger_id,
        r.total_fare,
        r.requested_date,

        e._source_system

    from events e
    left join rides r on e.ride_id = r.ride_id

)

select * from final
