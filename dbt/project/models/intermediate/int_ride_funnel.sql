with rides as (
    select * from {{ ref('int_rides_enriched') }}
),

funnel as (
    select requested_date_key, requested_date, city_code, service_type, city_service, ride_id, 1 as step_number, '01 Ride Requested' as funnel_step
    from rides

    union all

    select requested_date_key, requested_date, city_code, service_type, city_service, ride_id, 2, '02 Driver Accepted'
    from rides where accepted_at_utc is not null

    union all

    select requested_date_key, requested_date, city_code, service_type, city_service, ride_id, 3, '03 Driver Arrived'
    from rides where driver_arrived_at_utc is not null

    union all

    select requested_date_key, requested_date, city_code, service_type, city_service, ride_id, 4, '04 Ride Started'
    from rides where started_at_utc is not null

    union all

    select requested_date_key, requested_date, city_code, service_type, city_service, ride_id, 5, '05 Ride Completed'
    from rides where ride_status = 'COMPLETED'

    union all

    select requested_date_key, requested_date, city_code, service_type, city_service, ride_id, 6, '06 Payment Paid'
    from rides where payment_status = 'PAID'
)

select * from funnel
