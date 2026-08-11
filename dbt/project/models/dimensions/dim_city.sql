select
    {{ surrogate_key('city_id') }} as city_key,
    city_id,
    city_code,
    city_name,
    timezone,
    is_active,
    created_at_utc,
    created_at_jakarta,
    updated_at_utc,
    updated_at_jakarta
from {{ ref('stg_cities') }}
