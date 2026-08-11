{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='ride_id',
        partition_by={'field': 'requested_date', 'data_type': 'date'},
        cluster_by=['city_code', 'service_type', 'ride_status']
    )
}}

with rides as (
    select *
    from {{ ref('int_rides_enriched') }}
    {% if is_incremental() %}
    where updated_at_utc >= timestamp_sub(
        (
            select coalesce(max(updated_at_utc), timestamp('2000-01-01 00:00:00+00'))
            from {{ this }}
        ),
        interval {{ var('incremental_lookback_days', 2) }} day
    )
    {% endif %}
)

select
    ride_id,
    {{ surrogate_key('customer_id') }} as customer_key,
    {{ surrogate_key('driver_id') }} as driver_key,
    {{ surrogate_key('city_id') }} as city_key,
    {{ surrogate_key('pickup_zone_id') }} as pickup_zone_key,
    {{ surrogate_key('dropoff_zone_id') }} as dropoff_zone_key,

    -- Role-playing dim_date foreign keys.
    requested_date_key,
    accepted_date_key,
    driver_arrived_date_key,
    started_date_key,
    completed_date_key,
    cancelled_date_key,
    paid_date_key,

    customer_id,
    driver_id,
    city_id,
    pickup_zone_id,
    dropoff_zone_id,
    city_code,
    city_name,
    city_timezone,
    city_service,
    service_type,
    ride_status,
    pickup_zone_code,
    pickup_zone_name,
    pickup_zone_type,
    pickup_is_hotspot,
    dropoff_zone_code,
    dropoff_zone_name,
    dropoff_zone_type,
    customer_name,
    driver_name,
    driver_rating,

    -- Practical analytics fields remain in the fact for partitioning and BI.
    requested_at_utc,
    requested_at_jakarta,
    requested_date,
    requested_hour,
    day_of_week_name,
    is_weekend,
    peak_group,

    accepted_at_utc,
    accepted_at_jakarta,
    accepted_date,
    driver_arrived_at_utc,
    driver_arrived_at_jakarta,
    driver_arrived_date,
    started_at_utc,
    started_at_jakarta,
    started_date,
    completed_at_utc,
    completed_at_jakarta,
    completed_date,
    cancelled_at_utc,
    cancelled_at_jakarta,
    cancelled_date,
    cancelled_by,
    cancellation_reason,

    estimated_distance_km,
    actual_distance_km,
    estimated_duration_min,
    actual_duration_min,
    base_fare,
    distance_fare,
    time_fare,
    surge_multiplier,
    gross_fare,
    discount_amount,
    final_fare,

    payment_id,
    payment_method,
    payment_status,
    payment_amount,
    platform_fee,
    driver_earning,
    payment_failure_reason,
    paid_at_utc,
    paid_at_jakarta,
    paid_date,

    requested_rides,
    accepted_rides,
    driver_arrived_rides,
    started_rides,
    completed_rides,
    cancelled_rides,
    no_driver_rides,
    paid_rides,
    payment_failed_rides,
    gross_revenue,
    platform_revenue,
    driver_earnings,
    accept_delay_min,
    driver_arrival_delay_min,

    created_at_utc,
    created_at_jakarta,
    updated_at_utc,
    updated_at_jakarta,
    _ingested_at_utc,
    _ingested_at_jakarta,
    _batch_id,
    _source_system,
    _source_updated_at_utc,
    _source_updated_at_jakarta,
    _airflow_run_id
from rides
