select ride_id
from {{ ref('fct_rides') }}
where (accepted_at_utc is not null and accepted_at_utc < requested_at_utc)
   or (
       driver_arrived_at_utc is not null
       and accepted_at_utc is not null
       and driver_arrived_at_utc < accepted_at_utc
   )
   or (
       started_at_utc is not null
       and driver_arrived_at_utc is not null
       and started_at_utc < driver_arrived_at_utc
   )
   or (
       completed_at_utc is not null
       and started_at_utc is not null
       and completed_at_utc < started_at_utc
   )
