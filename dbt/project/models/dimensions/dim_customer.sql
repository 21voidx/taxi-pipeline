select
    {{ surrogate_key('customer_id') }} as customer_key,
    customer_id,
    {{ surrogate_key('registered_city_id') }} as registered_city_key,
    registered_city_id,
    customer_name,
    customer_status,
    created_at_utc,
    created_at_jakarta,
    updated_at_utc,
    updated_at_jakarta
from {{ ref('stg_customers') }}
