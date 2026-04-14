select
    r.room_id,
    r.room_category,
    r.base_price_per_night,
    count(b.booking_id) as times_booked,
    sum(b.total_total) as total_revenue_generated
from {{ ref('dim_rooms') }} r
left join {{ ref('fct_bookings') }} b on r.room_id = b.room_id
group by 1, 2, 3
order by 1