select
    metric_date,
    requested_hour,
    city_code,
    service_type,
    count(*) as row_count
from {{ ref('agg_hourly_city_service') }}
group by 1, 2, 3, 4
having count(*) > 1
