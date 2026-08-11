{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'metric_date', 'data_type': 'date'},
        cluster_by=['city_code', 'service_type', 'driver_id'],
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

rides as (
    select f.*
    from {{ ref('fct_rides') }} as f
    {% if is_incremental() %}
    inner join affected_dates as a
        on f.requested_date = a.metric_date
    {% endif %}
    where f.driver_id is not null
),

aggregated as (
    select
        requested_date_key as metric_date_key,
        requested_date as metric_date,
        driver_id,
        driver_name,
        city_code,
        service_type,
        city_service,
        any_value(driver_rating) as driver_rating,
        count(*) as assigned_rides,
        sum(completed_rides) as completed_rides,
        sum(cancelled_rides) as cancelled_rides,
        sum(gross_revenue) as gross_revenue,
        sum(driver_earnings) as driver_earnings,
        {{ safe_rate('sum(completed_rides)', 'count(*)') }} as driver_completion_rate,
        avg(accept_delay_min) as avg_accept_delay_min,
        avg(driver_arrival_delay_min) as avg_driver_arrival_min,
        max(updated_at_utc) as max_source_updated_at_utc
    from rides
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from aggregated
