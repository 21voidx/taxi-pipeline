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
    tags                = ['silver', 'staging', 'events']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  stg_ride_events
--  Purpose : Flatten JSON payload per event_type from ride_events.
--  JSON extraction uses the `extract_json_bq` macro.
-- ══════════════════════════════════════════════════════════════

with source as (

    select * from {{ source('bronze_pg', 'ride_events') }}

    {% if is_incremental() %}
    where occurred_at >= timestamp_sub(
        current_timestamp(),
        interval {{ var('incremental_lookback_days') }} day
    )
    {% endif %}

),

parsed as (

    select
        event_id,
        ride_id,
        event_type,
        occurred_at,
        date(occurred_at)  as occurred_date,

        -- ── Typed JSON extractions per event_type ─────────────────
        -- RIDE_REQUESTED  → surge (float), zone (string)
        safe_cast({{ extract_json_bq('event_payload', 'surge') }}  as float64)
                           as req_surge,
        {{ extract_json_bq('event_payload', 'zone') }}
                           as req_zone,

        -- RIDE_ACCEPTED   → driver_id (string)
        {{ extract_json_bq('event_payload', 'driver_id') }}
                           as acc_driver_id,

        -- RIDE_COMPLETED  → fare (float), distance (float)
        safe_cast({{ extract_json_bq('event_payload', 'fare') }}   as float64)
                           as cmp_fare,
        safe_cast({{ extract_json_bq('event_payload', 'distance') }} as float64)
                           as cmp_distance_km,

        -- RIDE_CANCELLED  → reason (string)
        {{ extract_json_bq('event_payload', 'reason') }}
                           as can_reason,

        event_payload,
        _source_system

    from source

)

select * from parsed
