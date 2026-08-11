select
    {{ surrogate_key('zone_id') }} as zone_key,
    zone_id,
    {{ surrogate_key('city_id') }} as city_key,
    city_id,
    zone_code,
    zone_name,
    zone_type,
    is_hotspot,
    is_active,
    created_at_utc,
    created_at_jakarta,
    updated_at_utc,
    updated_at_jakarta
from {{ ref('stg_zones') }}
