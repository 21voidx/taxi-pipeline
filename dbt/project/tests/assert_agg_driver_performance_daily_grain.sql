select
    metric_date,
    driver_id,
    city_code,
    service_type,
    count(*) as row_count
from {{ ref('agg_driver_performance_daily') }}
group by 1, 2, 3, 4
having count(*) > 1
