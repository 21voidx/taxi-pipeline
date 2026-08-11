select ride_id
from {{ ref('fct_rides') }}
where requested_at_jakarta != datetime(requested_at_utc, 'Asia/Jakarta')
   or requested_date != date(requested_at_utc, 'Asia/Jakarta')
   or requested_hour != extract(hour from datetime(requested_at_utc, 'Asia/Jakarta'))
   or (
        accepted_at_utc is not null
        and (
            accepted_date is null
            or accepted_date != date(accepted_at_utc, 'Asia/Jakarta')
        )
   )
   or (
        driver_arrived_at_utc is not null
        and (
            driver_arrived_date is null
            or driver_arrived_date != date(driver_arrived_at_utc, 'Asia/Jakarta')
        )
   )
   or (
        started_at_utc is not null
        and (
            started_date is null
            or started_date != date(started_at_utc, 'Asia/Jakarta')
        )
   )
   or (
        completed_at_utc is not null
        and (
            completed_date is null
            or completed_date != date(completed_at_utc, 'Asia/Jakarta')
        )
   )
   or (
        cancelled_at_utc is not null
        and (
            cancelled_date is null
            or cancelled_date != date(cancelled_at_utc, 'Asia/Jakarta')
        )
   )
   or (
        paid_at_utc is not null
        and (
            paid_date is null
            or paid_date != date(paid_at_utc, 'Asia/Jakarta')
        )
   )
