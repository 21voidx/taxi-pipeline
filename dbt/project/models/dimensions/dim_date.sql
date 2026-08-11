{{ config(materialized='table') }}

with date_spine as (
    select calendar_date
    from unnest(
        generate_date_array(
            date('{{ var("date_spine_start", "2024-01-01") }}'),
            date('{{ var("date_spine_end", "2030-12-31") }}'),
            interval 1 day
        )
    ) as calendar_date
)

select
    cast(format_date('%Y%m%d', calendar_date) as int64) as date_key,
    calendar_date as full_date,

    extract(dayofweek from calendar_date) as day_of_week_number,
    format_date('%A', calendar_date) as day_name,
    extract(day from calendar_date) as day_of_month,
    extract(dayofyear from calendar_date) as day_of_year,

    extract(isoweek from calendar_date) as iso_week_of_year,
    extract(isoyear from calendar_date) as iso_year_number,
    format_date('%G-W%V', calendar_date) as iso_year_week,
    date_trunc(calendar_date, week(monday)) as week_start_date,
    date_add(
        date_trunc(calendar_date, week(monday)),
        interval 6 day
    ) as week_end_date,

    extract(month from calendar_date) as month_number,
    format_date('%B', calendar_date) as month_name,
    format_date('%Y-%m', calendar_date) as year_month,
    date_trunc(calendar_date, month) as month_start_date,
    last_day(calendar_date, month) as month_end_date,

    extract(quarter from calendar_date) as quarter_number,
    concat(
        cast(extract(year from calendar_date) as string),
        '-Q',
        cast(extract(quarter from calendar_date) as string)
    ) as year_quarter,
    date_trunc(calendar_date, quarter) as quarter_start_date,
    last_day(calendar_date, quarter) as quarter_end_date,

    extract(year from calendar_date) as year_number,
    date_trunc(calendar_date, year) as year_start_date,
    last_day(calendar_date, year) as year_end_date,

    extract(dayofweek from calendar_date) in (1, 7) as is_weekend,
    calendar_date = date_trunc(calendar_date, month) as is_month_start,
    calendar_date = last_day(calendar_date, month) as is_month_end,
    calendar_date = date_trunc(calendar_date, quarter) as is_quarter_start,
    calendar_date = last_day(calendar_date, quarter) as is_quarter_end,
    calendar_date = date_trunc(calendar_date, year) as is_year_start,
    calendar_date = last_day(calendar_date, year) as is_year_end
from date_spine
