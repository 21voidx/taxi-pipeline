with ranked as (
    select
        zone_id,
        city_id,
        upper(trim(zone_code)) as zone_code,
        trim(zone_name) as zone_name,
        upper(trim(zone_type)) as zone_type,
        is_hotspot,
        is_active,
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
            partition by zone_id
            order by updated_at desc, _ingested_at desc
        ) as _row_number
    from {{ source('raw_ride_hailing', 'raw_zones') }}
)

select * except (_row_number)
from ranked
where _row_number = 1
