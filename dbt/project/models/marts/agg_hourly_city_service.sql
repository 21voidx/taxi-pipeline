{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'metric_date', 'data_type': 'date'},
        cluster_by=['city_code', 'service_type', 'requested_hour'],
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
),

aggregated as (
    select
        requested_date_key as metric_date_key,
        requested_date as metric_date,
        requested_hour,
        day_of_week_name,
        is_weekend,
        peak_group,
        city_code,
        city_name,
        service_type,
        city_service,
        sum(requested_rides) as requested_rides,
        sum(accepted_rides) as accepted_rides,
        sum(completed_rides) as completed_rides,
        sum(cancelled_rides) as cancelled_rides,
        sum(no_driver_rides) as no_driver_rides,
        sum(gross_revenue) as gross_revenue,
        {{ safe_rate('sum(completed_rides)', 'sum(requested_rides)') }} as completion_rate,
        {{ safe_rate('sum(cancelled_rides)', 'sum(requested_rides)') }} as cancellation_rate,
        {{ safe_rate('sum(accepted_rides)', 'sum(requested_rides)') }} as acceptance_rate,
        avg(surge_multiplier) as avg_surge_multiplier,
        avg(accept_delay_min) as avg_accept_delay_min,
        avg(driver_arrival_delay_min) as avg_driver_arrival_min,
        max(updated_at_utc) as max_source_updated_at_utc
    from rides
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

select * from aggregated
