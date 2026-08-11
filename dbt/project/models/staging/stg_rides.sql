with ranked as (
    select
        ride_id,
        customer_id,
        driver_id,
        city_id,
        upper(trim(service_type)) as service_type,
        pickup_zone_id,
        dropoff_zone_id,
        upper(trim(ride_status)) as ride_status,

        requested_at as requested_at_utc,
        datetime(requested_at, 'Asia/Jakarta') as requested_at_jakarta,
        date(requested_at, 'Asia/Jakarta') as requested_date,
        extract(hour from datetime(requested_at, 'Asia/Jakarta')) as requested_hour,

        accepted_at as accepted_at_utc,
        datetime(accepted_at, 'Asia/Jakarta') as accepted_at_jakarta,
        date(accepted_at, 'Asia/Jakarta') as accepted_date,

        driver_arrived_at as driver_arrived_at_utc,
        datetime(driver_arrived_at, 'Asia/Jakarta') as driver_arrived_at_jakarta,
        date(driver_arrived_at, 'Asia/Jakarta') as driver_arrived_date,

        started_at as started_at_utc,
        datetime(started_at, 'Asia/Jakarta') as started_at_jakarta,
        date(started_at, 'Asia/Jakarta') as started_date,

        completed_at as completed_at_utc,
        datetime(completed_at, 'Asia/Jakarta') as completed_at_jakarta,
        date(completed_at, 'Asia/Jakarta') as completed_date,

        cancelled_at as cancelled_at_utc,
        datetime(cancelled_at, 'Asia/Jakarta') as cancelled_at_jakarta,
        date(cancelled_at, 'Asia/Jakarta') as cancelled_date,

        upper(trim(cancelled_by)) as cancelled_by,
        upper(trim(cancellation_reason)) as cancellation_reason,
        cast(estimated_distance_km as numeric) as estimated_distance_km,
        cast(actual_distance_km as numeric) as actual_distance_km,
        cast(estimated_duration_min as numeric) as estimated_duration_min,
        cast(actual_duration_min as numeric) as actual_duration_min,
        cast(base_fare as numeric) as base_fare,
        cast(distance_fare as numeric) as distance_fare,
        cast(time_fare as numeric) as time_fare,
        cast(surge_multiplier as numeric) as surge_multiplier,
        cast(gross_fare as numeric) as gross_fare,
        cast(discount_amount as numeric) as discount_amount,
        cast(final_fare as numeric) as final_fare,
        cast(status_version as int64) as status_version,

        created_at as created_at_utc,
        datetime(created_at, 'Asia/Jakarta') as created_at_jakarta,
        updated_at as updated_at_utc,
        datetime(updated_at, 'Asia/Jakarta') as updated_at_jakarta,
        _ingested_at as _ingested_at_utc,
        datetime(_ingested_at, 'Asia/Jakarta') as _ingested_at_jakarta,
        _batch_id,
        _source_system,
        _source_updated_at as _source_updated_at_utc,
        datetime(_source_updated_at, 'Asia/Jakarta') as _source_updated_at_jakarta,
        _airflow_run_id,

        row_number() over (
            partition by ride_id
            order by updated_at desc, status_version desc, _ingested_at desc
        ) as _row_number
    from {{ source('raw_ride_hailing', 'raw_rides') }}
)

select * except (_row_number)
from ranked
where _row_number = 1
