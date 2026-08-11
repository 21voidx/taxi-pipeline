select ride_id
from {{ ref('fct_rides') }}
where requested_date_key != cast(format_date('%Y%m%d', requested_date) as int64)
   or (
        accepted_date is not null
        and (
            accepted_date_key is null
            or accepted_date_key != cast(format_date('%Y%m%d', accepted_date) as int64)
        )
   )
   or (
        driver_arrived_date is not null
        and (
            driver_arrived_date_key is null
            or driver_arrived_date_key != cast(format_date('%Y%m%d', driver_arrived_date) as int64)
        )
   )
   or (
        started_date is not null
        and (
            started_date_key is null
            or started_date_key != cast(format_date('%Y%m%d', started_date) as int64)
        )
   )
   or (
        completed_date is not null
        and (
            completed_date_key is null
            or completed_date_key != cast(format_date('%Y%m%d', completed_date) as int64)
        )
   )
   or (
        cancelled_date is not null
        and (
            cancelled_date_key is null
            or cancelled_date_key != cast(format_date('%Y%m%d', cancelled_date) as int64)
        )
   )
   or (
        paid_date is not null
        and (
            paid_date_key is null
            or paid_date_key != cast(format_date('%Y%m%d', paid_date) as int64)
        )
   )
