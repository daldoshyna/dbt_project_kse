select
    cast(date_trunc('week', check_in_date) as date) as report_week,
    count(distinct booking_id) as total_bookings,
    sum(total_total) as total_revenue,
    round(avg(stay_duration_nights), 2) as avg_length_of_stay
from {{ ref('fct_bookings') }}
where status_id not in (select status_id from {{ ref('dim_booking_statuses') }} where booking_status = 'Cancelled')
group by 1