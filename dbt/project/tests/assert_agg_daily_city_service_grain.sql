select
    metric_date,
    city_code,
    service_type,
    count(*) as row_count
from {{ ref('agg_daily_city_service') }}
group by 1, 2, 3
having count(*) > 1
