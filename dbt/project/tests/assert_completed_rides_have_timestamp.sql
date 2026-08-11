select ride_id
from {{ ref('fct_rides') }}
where ride_status = 'COMPLETED'
  and completed_at_utc is null
