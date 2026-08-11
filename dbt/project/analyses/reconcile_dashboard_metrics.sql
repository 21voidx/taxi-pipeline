-- Use this query to reconcile Looker scorecards with BigQuery.
-- metric_date is already derived from Asia/Jakarta business time.
select
    sum(requested_rides) as requested_rides,
    sum(completed_rides) as completed_rides,
    sum(cancelled_rides) as cancelled_rides,
    sum(gross_revenue) as gross_revenue,
    safe_divide(sum(completed_rides), sum(requested_rides)) as weighted_completion_rate,
    safe_divide(sum(cancelled_rides), sum(requested_rides)) as weighted_cancellation_rate
from {{ ref('agg_daily_city_service') }}
where metric_date between
    date_sub(current_date('Asia/Jakarta'), interval 30 day)
    and current_date('Asia/Jakarta')
