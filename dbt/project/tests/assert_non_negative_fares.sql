select ride_id
from {{ ref('fct_rides') }}
where final_fare < 0
   or gross_fare < 0
   or discount_amount < 0
