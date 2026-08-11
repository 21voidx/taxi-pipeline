select
    metric_date,
    city_code,
    service_type,
    step_number,
    count(*) as row_count
from {{ ref('agg_ride_funnel_steps_daily') }}
group by 1, 2, 3, 4
having count(*) > 1
