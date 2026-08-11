-- depends_on: {{ ref('fct_rides') }}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'metric_date', 'data_type': 'date'},
        cluster_by=['city_code', 'service_type', 'step_number'],
        on_schema_change='sync_all_columns'
    )
}}

{% if is_incremental() %}
with affected_dates as (
    select distinct requested_date as metric_date
    from {{ ref('fct_rides') }}
    where updated_at_utc >= timestamp_sub(
        current_timestamp(),
        interval {{ var('agg_change_lookback_days', 7) }} day
    )
),
{% else %}
with
{% endif %}

funnel as (
    select f.*
    from {{ ref('int_ride_funnel') }} as f
    {% if is_incremental() %}
    inner join affected_dates as a
        on f.requested_date = a.metric_date
    {% endif %}
),

counts as (
    select
        requested_date_key as metric_date_key,
        requested_date as metric_date,
        city_code,
        service_type,
        city_service,
        step_number,
        funnel_step,
        count(distinct ride_id) as ride_count
    from funnel
    group by 1, 2, 3, 4, 5, 6, 7
),

with_windows as (
    select
        *,
        lag(ride_count) over (
            partition by metric_date_key, city_code, service_type
            order by step_number
        ) as previous_step_ride_count,
        first_value(ride_count) over (
            partition by metric_date_key, city_code, service_type
            order by step_number
        ) as starting_ride_count
    from counts
)

select
    *,
    {{ safe_rate('ride_count', 'starting_ride_count') }} as conversion_from_start,
    {{ safe_rate('ride_count', 'previous_step_ride_count') }} as conversion_from_previous_step,
    1 - {{ safe_rate('ride_count', 'previous_step_ride_count') }} as dropoff_from_previous_step
from with_windows
