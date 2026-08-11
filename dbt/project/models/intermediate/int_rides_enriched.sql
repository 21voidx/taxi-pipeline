with rides as (
    select * from {{ ref('stg_rides') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

cities as (
    select * from {{ ref('stg_cities') }}
),

zones as (
    select * from {{ ref('stg_zones') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

joined as (
    select
        r.*,

        requested_calendar.date_key as requested_date_key,
        requested_calendar.day_name as requested_day_name,
        requested_calendar.is_weekend as requested_is_weekend,

        accepted_calendar.date_key as accepted_date_key,
        arrived_calendar.date_key as driver_arrived_date_key,
        started_calendar.date_key as started_date_key,
        completed_calendar.date_key as completed_date_key,
        cancelled_calendar.date_key as cancelled_date_key,

        pay.payment_id,
        pay.payment_method,
        pay.payment_status,
        pay.payment_amount,
        pay.platform_fee,
        pay.driver_earning,
        pay.failure_reason as payment_failure_reason,
        pay.paid_at_utc,
        pay.paid_at_jakarta,
        pay.paid_date,
        paid_calendar.date_key as paid_date_key,
        pay.updated_at_utc as payment_updated_at_utc,

        c.city_code,
        c.city_name,
        c.timezone as city_timezone,
        pz.zone_code as pickup_zone_code,
        pz.zone_name as pickup_zone_name,
        pz.zone_type as pickup_zone_type,
        pz.is_hotspot as pickup_is_hotspot,
        dz.zone_code as dropoff_zone_code,
        dz.zone_name as dropoff_zone_name,
        dz.zone_type as dropoff_zone_type,
        cu.customer_name,
        cu.customer_status,
        d.driver_name,
        d.driver_status,
        d.rating as driver_rating
    from rides r
    left join payments pay on pay.ride_id = r.ride_id
    left join cities c on c.city_id = r.city_id
    left join zones pz on pz.zone_id = r.pickup_zone_id
    left join zones dz on dz.zone_id = r.dropoff_zone_id
    left join customers cu on cu.customer_id = r.customer_id
    left join drivers d on d.driver_id = r.driver_id

    -- One dim_date model is reused in several lifecycle roles.
    left join dates requested_calendar
        on requested_calendar.full_date = r.requested_date
    left join dates accepted_calendar
        on accepted_calendar.full_date = r.accepted_date
    left join dates arrived_calendar
        on arrived_calendar.full_date = r.driver_arrived_date
    left join dates started_calendar
        on started_calendar.full_date = r.started_date
    left join dates completed_calendar
        on completed_calendar.full_date = r.completed_date
    left join dates cancelled_calendar
        on cancelled_calendar.full_date = r.cancelled_date
    left join dates paid_calendar
        on paid_calendar.full_date = pay.paid_date
)

select
    ride_id,
    customer_id,
    driver_id,
    city_id,
    pickup_zone_id,
    dropoff_zone_id,
    service_type,
    ride_status,
    city_code,
    city_name,
    city_timezone,
    concat(city_code, ' - ', service_type) as city_service,
    pickup_zone_code,
    pickup_zone_name,
    pickup_zone_type,
    pickup_is_hotspot,
    dropoff_zone_code,
    dropoff_zone_name,
    dropoff_zone_type,
    customer_name,
    customer_status,
    driver_name,
    driver_status,
    driver_rating,

    requested_date_key,
    accepted_date_key,
    driver_arrived_date_key,
    started_date_key,
    completed_date_key,
    cancelled_date_key,
    paid_date_key,

    requested_at_utc,
    requested_at_jakarta,
    requested_date,
    requested_hour,
    requested_day_name as day_of_week_name,
    requested_is_weekend as is_weekend,
    case
        when not requested_is_weekend
         and requested_hour between 6 and 9 then 'MORNING_PEAK'
        when not requested_is_weekend
         and requested_hour between 16 and 20 then 'EVENING_PEAK'
        else 'NON_PEAK'
    end as peak_group,

    accepted_at_utc,
    accepted_at_jakarta,
    accepted_date,
    driver_arrived_at_utc,
    driver_arrived_at_jakarta,
    driver_arrived_date,
    started_at_utc,
    started_at_jakarta,
    started_date,
    completed_at_utc,
    completed_at_jakarta,
    completed_date,
    cancelled_at_utc,
    cancelled_at_jakarta,
    cancelled_date,
    cancelled_by,
    cancellation_reason,

    estimated_distance_km,
    actual_distance_km,
    estimated_duration_min,
    actual_duration_min,
    base_fare,
    distance_fare,
    time_fare,
    surge_multiplier,
    gross_fare,
    discount_amount,
    final_fare,
    status_version,

    payment_id,
    payment_method,
    payment_status,
    payment_amount,
    platform_fee,
    driver_earning,
    payment_failure_reason,
    paid_at_utc,
    paid_at_jakarta,
    paid_date,

    1 as requested_rides,
    if(accepted_at_utc is not null, 1, 0) as accepted_rides,
    if(driver_arrived_at_utc is not null, 1, 0) as driver_arrived_rides,
    if(started_at_utc is not null, 1, 0) as started_rides,
    if(ride_status = 'COMPLETED', 1, 0) as completed_rides,
    if(ride_status = 'CANCELLED', 1, 0) as cancelled_rides,
    if(ride_status = 'NO_DRIVER', 1, 0) as no_driver_rides,
    if(payment_status = 'PAID', 1, 0) as paid_rides,
    if(payment_status = 'FAILED', 1, 0) as payment_failed_rides,
    if(ride_status = 'COMPLETED', final_fare, 0) as gross_revenue,
    if(payment_status = 'PAID', platform_fee, 0) as platform_revenue,
    if(payment_status = 'PAID', driver_earning, 0) as driver_earnings,

    if(
        accepted_at_utc is not null,
        timestamp_diff(accepted_at_utc, requested_at_utc, second) / 60.0,
        null
    ) as accept_delay_min,
    if(
        driver_arrived_at_utc is not null and accepted_at_utc is not null,
        timestamp_diff(driver_arrived_at_utc, accepted_at_utc, second) / 60.0,
        null
    ) as driver_arrival_delay_min,

    created_at_utc,
    created_at_jakarta,
    greatest(
        updated_at_utc,
        coalesce(payment_updated_at_utc, updated_at_utc)
    ) as updated_at_utc,
    datetime(
        greatest(
            updated_at_utc,
            coalesce(payment_updated_at_utc, updated_at_utc)
        ),
        'Asia/Jakarta'
    ) as updated_at_jakarta,
    _ingested_at_utc,
    _ingested_at_jakarta,
    _batch_id,
    _source_system,
    _source_updated_at_utc,
    _source_updated_at_jakarta,
    _airflow_run_id
from joined
