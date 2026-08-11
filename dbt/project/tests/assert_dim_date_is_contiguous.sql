with summary as (
    select
        min(full_date) as minimum_date,
        max(full_date) as maximum_date,
        count(*) as actual_date_count
    from {{ ref('dim_date') }}
)

select *
from summary
where actual_date_count != date_diff(maximum_date, minimum_date, day) + 1
