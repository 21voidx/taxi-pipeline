with ranked as (
    select
        driver_id,
        trim(driver_name) as driver_name,
        city_id,
        upper(trim(service_type)) as service_type,
        upper(trim(driver_status)) as driver_status,
        cast(rating as numeric) as rating,
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
            partition by driver_id
            order by updated_at desc, _ingested_at desc
        ) as _row_number
    from {{ source('raw_ride_hailing', 'raw_drivers') }}
)

select * except (_row_number)
from ranked
where _row_number = 1
