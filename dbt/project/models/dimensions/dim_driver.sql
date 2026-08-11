with drivers as (
    select * from {{ ref('stg_drivers') }}
),
vehicles as (
    select * from {{ ref('stg_vehicles') }}
)

select
    {{ surrogate_key('d.driver_id') }} as driver_key,
    d.driver_id,
    {{ surrogate_key('d.city_id') }} as city_key,
    d.city_id,
    d.driver_name,
    d.service_type,
    d.driver_status,
    d.rating,
    v.vehicle_id,
    v.vehicle_type,
    v.vehicle_year,
    v.vehicle_status,
    d.created_at_utc,
    d.created_at_jakarta,
    greatest(
        d.updated_at_utc,
        coalesce(v.updated_at_utc, d.updated_at_utc)
    ) as updated_at_utc,
    datetime(
        greatest(
            d.updated_at_utc,
            coalesce(v.updated_at_utc, d.updated_at_utc)
        ),
        'Asia/Jakarta'
    ) as updated_at_jakarta
from drivers d
left join vehicles v on v.driver_id = d.driver_id
